use crate::alias::AMessage;
use crate::archive::{ArchiveChannel, Asset, ServerArchive};
use anyhow::Result;
use futures::{stream, StreamExt};
use reqwest::Client;
use serde_json::{from_str, to_string_pretty};
use serenity::all::{ChannelType, Context, EventHandler, GatewayIntents, GetMessages, Guild, GuildChannel, GuildId, Message, MessageId, Ready, ThreadsData, Timestamp};
use serenity::async_trait;
use std::collections::HashSet;
use std::env::args;
use std::fs::{write, File};
use std::io::{stdout, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use std::{env, fs};
use tokio::spawn;
use tokio::time::{sleep, timeout};

#[tokio::main]
async fn main() {
    println!("Starting server, supply token in args or in env DISCORD_TOKEN");

    let args = args();
    let token = if args.len() == 2 {
        let token = args.into_iter().nth(1).expect("Incorrect token args");
        token
    } else {
        env::var("DISCORD_TOKEN").expect("Expected a token in the environment")
    };

    let intents = GatewayIntents::GUILD_MESSAGES | GatewayIntents::MESSAGE_CONTENT | GatewayIntents::GUILDS;

    let mut client = serenity::Client::builder(&token, intents)
        .event_handler(Handler)
        .await
        .expect("Err creating client");

    let shard_manager = client.shard_manager.clone();
    spawn(async move {
        tokio::signal::ctrl_c().await.expect("Could not register ctrl+c handler");
        println!("Ctrl-c received. Shutting down client...");
        shard_manager.shutdown_all().await;
    });

    if let Err(why) = client.start().await {
        println!("Client error: {why:?}");
    }
}

struct Handler;

#[async_trait]
impl EventHandler for Handler {
    async fn guild_create(&self, _ctx: Context, guild: Guild, is_new: Option<bool>) {
        if let Some(c) = is_new {
            if c {
                println!("Just connected to {}", guild.name);
            }
        }
    }

    async fn message(&self, ctx: Context, new_message: Message) {
        if new_message.content == "!pull_backup" {
            let guild_id = new_message.guild_id.unwrap();

            pull_backup(ctx.clone(), guild_id, new_message.timestamp, new_message.id)
                .await
                .unwrap();
        }
    }

    async fn ready(&self, ctx: Context, data_about_bot: Ready) {
        println!("Bot active and waiting for !pull_backup message");
        let guild_names: Vec<String> = stream::iter(data_about_bot.guilds)
            .map(|g| (g, ctx.clone()))
            .map(|(g, ctx)| async move {
                ctx.http
                    .get_guild(g.id)
                    .await
                    .map(|guild| guild.name)
                    .unwrap_or("Unknown".into())
            })
            .buffer_unordered(3)
            .collect()
            .await;

        for name in guild_names {
            println!("Connected to {name}");
        }
    }
}

pub async fn pull_backup(ctx: Context, guild_id: GuildId, time: Timestamp, ref_message_id: MessageId) -> Result<()> {
    // create root
    let name = ctx
        .http
        .get_guild(guild_id)
        .await
        .map(|e| e.name)
        .unwrap_or("Unknown".into());
    let server_name = format!("{}-{}-{}", name, guild_id, time);
    let san_name = sanitise_file_name::sanitise(&server_name);
    let root = PathBuf::from(san_name);
    fs::create_dir(&root)?;

    let mut server_archive = ServerArchive::default();

    let partial = ctx.http.get_guild(guild_id).await?;
    println!("pulling backup for {}", partial.name);

    // download main channels
    println!("<<<>>> <<<>>> DOWNLOADING CHANNELS");
    let channels = partial
        .channels(&ctx.http)
        .await?
        .into_iter()
        .map(|(_, p)| p)
        .collect::<Vec<GuildChannel>>();

    let ddl = async |gc: &GuildChannel| -> Result<ArchiveChannel> {
        let stats = Arc::new(AtomicU32::new(0));

        let in_pro = {
            let ctx = ctx.clone();
            let gc = gc.clone();
            let stats = stats.clone();
            spawn(async move { archive_channel(&ctx, &gc, stats, ref_message_id).await })
        };

        loop {
            if in_pro.is_finished() {
                println!("\rLOADING {}", stats.load(Ordering::SeqCst));
                break;
            }
            print!("\rLOADING {}", stats.load(Ordering::SeqCst));
            stdout().flush().unwrap();
            sleep(Duration::from_millis(250)).await;
        }

        in_pro.await?
    };

    for gc in channels.iter() {
        println!("{} <><> {} <><> {:?}", gc.id, gc.name, gc.kind);
        let archived = ddl(&gc).await?;
        server_archive.main_channels.push(archived);
    }

    println!("<<<>>> <<<>>> DOWNLOADING THREADS");
    let threads = get_all_threads(&ctx, &guild_id, &channels).await?;
    for gc in threads {
        println!("{} <><> {} <><> {:?}", gc.id, gc.name, gc.kind);
        let archived = ddl(&gc).await?;
        server_archive.threads.push(archived);
    }

    let ser = to_string_pretty(&server_archive)?;
    write(root.join("ServerData.dsrip"), &ser)?;

    // validate
    let _: ServerArchive = from_str(&ser).expect("failed to reverse");

    solve_attachments(&root, &mut server_archive).await?;

    println!("\n<><> END ARCHIVE <><>");

    Ok(())
}

const SYNC: usize = 20;
const TIMEOUT: Duration = Duration::from_secs(120);
pub async fn solve_attachments(root: &Path, server_archive: &mut ServerArchive) -> Result<()> {
    let asset_dir = root.join("assets");
    if !asset_dir.exists() {
        fs::create_dir(&asset_dir)?;
    }

    let mut all_attachments = vec![];
    let mut total_bytes: u64 = 0;
    for c in server_archive
        .main_channels
        .iter()
        .chain(server_archive.threads.iter())
    {
        for m in c.messages.iter() {
            for a in m.attachments.iter() {
                all_attachments.push(a.clone());
                total_bytes += a.size as u64;
            }
        }
    }

    let amount = all_attachments.len() as u64;
    println!(
        "<<<>>> <<<>>> DOWNLOADING ATTACHMENTS, Count = {}, Size = {}Gb",
        amount,
        total_bytes as f64 / 1_000_000_000.0
    );

    async fn download_task(
        url: String,
        client: Arc<Client>,
        b: Arc<AtomicU64>,
        cc: Arc<AtomicU64>,
    ) -> Result<Vec<u8>> {
        let resp = client.get(url).send().await?;
        let bytes = resp.bytes().await?;
        b.fetch_add(bytes.len() as u64, Ordering::SeqCst);
        cc.fetch_add(1, Ordering::SeqCst);
        Ok(bytes.to_vec())
    }

    let bytes = Arc::new(AtomicU64::new(0));
    let bc = bytes.clone();
    let count = Arc::new(AtomicU64::new(0));
    let cc = count.clone();

    let kill = Arc::new(AtomicBool::new(false));
    let kill_c = kill.clone();
    let prog_tracker = spawn(async move {
        let mut fl = 0;
        let mut led = false;
        loop {
            if kill_c.load(Ordering::SeqCst) {
                led = true;
            }
            let prog = bc.load(Ordering::SeqCst);
            let c = cc.load(Ordering::SeqCst);
            fl += 1;
            if fl > 3 {
                fl = 1
            };
            print!(
                "\r[{:.5}] {} of {} {}      ",
                prog as f64 / total_bytes as f64,
                c,
                amount,
                "*".repeat(fl)
            );
            if led {
                break;
            }
            stdout().flush().unwrap();
            sleep(Duration::from_millis(250)).await;
        }
    });

    let client = Arc::new(Client::new());
    let assets: Vec<Asset> = stream::iter(all_attachments)
        .map(|a| {
            (
                a,
                client.clone(),
                bytes.clone(),
                asset_dir.clone(),
                count.clone(),
            )
        })
        .map(|(a, c, b, r, cc)| async move {
            let a: Asset = timeout(TIMEOUT, download_task(a.url.clone(), c, b, cc))
                .await
                .map_err(|_| "timeout".to_string())
                .and_then(|i| i.map_err(|e| e.to_string()))
                .and_then(|d| {
                    let pth = r.join(Asset::name(&a));
                    write(&pth, d)
                        .map(|_| pth.clone())
                        .map_err(|e| e.to_string())
                })
                .map_or_else(
                    |err| Asset {
                        metadata: a.clone(),
                        contents_ref: Err(err),
                    },
                    |pth| Asset {
                        metadata: a.clone(),
                        contents_ref: Ok(pth),
                    },
                );
            a
        })
        .buffer_unordered(SYNC) // Process 20 at a time
        .collect()
        .await;

    kill.store(true, Ordering::SeqCst);
    prog_tracker.await?;

    let failed_assets = assets
        .iter()
        .filter_map(|f| {
            if f.contents_ref.is_err() {
                let n = Asset::name(&f.metadata);
                Some((n, f.contents_ref.clone().unwrap_err()))
            } else {
                None
            }
        })
        .collect::<Vec<(String, String)>>();
    if !failed_assets.is_empty() {
        let mut file = File::create(root.join("FailedDownloadInfo.json"))?;
        for (p, e) in failed_assets {
            let s = format!("{p} | {e}\n");
            let mut b = s.into_bytes();
            file.write_all(&mut b)?;
        }
    }

    write(
        root.join(Path::new("AssetData.assdat")),
        to_string_pretty(&assets)?,
    )?;

    Ok(())
}

pub async fn get_all_threads(
    ctx: &Context,
    guild_id: &GuildId,
    channels: &Vec<GuildChannel>,
) -> Result<Vec<GuildChannel>> {
    let mut threads = vec![];
    let mut id_set = HashSet::new();

    let mut append = |v: ThreadsData| {
        if v.has_more {
            eprintln!("\n\n\t\t <><> THREADS DATA HAS MORE");
        }
        for c in v.threads {
            if id_set.contains(&c.id) {
                eprintln!("\n\n\t\t <><> ID SET OVERRIDE {}", c.name);
            } else {
                id_set.insert(c.id);
                threads.push(c);
            }
        }
    };

    let active = guild_id.get_active_threads(&ctx.http).await?;
    append(active);

    const ALLOWED: [ChannelType; 4] = [
        ChannelType::Text,
        ChannelType::News,
        ChannelType::Forum,
        ChannelType::Directory,
    ];

    for gc in channels.iter() {
        if !ALLOWED.contains(&gc.kind) {
            continue;
        };

        let a = gc
            .id
            .get_archived_private_threads(&ctx.http, None, None)
            .await?;
        let b = gc
            .id
            .get_archived_public_threads(&ctx.http, None, None)
            .await?;
        append(a);
        append(b);
    }

    Ok(threads)
}

pub async fn archive_channel(
    ctx: &Context,
    gc: &GuildChannel,
    stats_handle: Arc<AtomicU32>,
    ref_message_id: MessageId,
) -> Result<ArchiveChannel> {
    let mut messages = vec![];
    match gc.kind {
        ChannelType::Text
        | ChannelType::Private
        | ChannelType::Voice
        | ChannelType::GroupDm
        | ChannelType::News
        | ChannelType::NewsThread
        | ChannelType::PublicThread
        | ChannelType::PrivateThread
        | ChannelType::Stage
        | ChannelType::Directory => {
            let mut lmi = None;
            'query: loop {
                let mut get = GetMessages::new().limit(100);
                if let Some(id) = lmi {
                    get = get.before(id);
                } else {
                    get = get.before(ref_message_id);
                }
                let query = gc.id.messages(&ctx.http, get).await?;
                if query.is_empty() {
                    break 'query;
                } else {
                    lmi = Some(query.last().unwrap().id);
                    stats_handle.fetch_add(query.len() as u32, Ordering::SeqCst);
                    let mut new: Vec<AMessage> = query.into_iter().map(|p| p.into()).collect();
                    messages.append(&mut new);
                }
            }
        }

        ChannelType::Category | ChannelType::Forum => {}

        ChannelType::Unknown(_) | _ => {
            return Err(anyhow::anyhow!("Unknown type"));
        }
    }

    Ok(ArchiveChannel {
        channel_metadata: gc.clone().into(),
        messages,
    })
}

mod archive {
    use crate::alias::{AAttachment, AGuildChannel, AMessage};
    use serde::{Deserialize, Serialize};
    use std::path::PathBuf;

    #[derive(Serialize, Deserialize, Clone, Debug, Default)]
    pub struct ServerArchive {
        pub main_channels: Vec<ArchiveChannel>,
        pub threads: Vec<ArchiveChannel>,
        pub downloaded_assets: Vec<Asset>,
        pub failed_assets: Vec<Asset>,
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct ArchiveChannel {
        pub channel_metadata: AGuildChannel,
        pub messages: Vec<AMessage>,
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct Asset {
        pub metadata: AAttachment,
        // reference to the relative path of the asset
        pub contents_ref: Result<PathBuf, String>,
    }
    impl Asset {
        pub fn name(a: &AAttachment) -> String {
            format!("{}-{}", a.id, sanitise_file_name::sanitise(&a.filename))
        }
    }
}

// needed to avoid serialization errors
mod alias {
    use serde::{Deserialize, Serialize};
    use serenity::all::{
        Attachment, AttachmentId, ChannelId, ChannelType, GuildChannel, GuildId, Message,
        MessageId, MessageType, ThreadMetadata, Timestamp, User, UserId,
    };

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct AAttachment {
        pub id: AttachmentId,
        pub filename: String,
        pub description: Option<String>,
        pub height: Option<u32>,
        pub proxy_url: String,
        pub size: u32,
        pub url: String,
        pub width: Option<u32>,
        pub content_type: Option<String>,
        pub duration_secs: Option<f64>,
    }
    impl From<Attachment> for AAttachment {
        fn from(v: Attachment) -> Self {
            AAttachment {
                id: v.id,
                filename: v.filename,
                description: v.description,
                height: v.height,
                proxy_url: v.proxy_url,
                size: v.size,
                url: v.url,
                width: v.width,
                content_type: v.content_type,
                duration_secs: v.duration_secs,
            }
        }
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct AMessage {
        /// The unique Id of the message. Can be used to calculate the creation date of the message.
        pub id: MessageId,
        /// The Id of the [`Channel`] that the message was sent to.
        pub channel_id: ChannelId,
        /// The user that sent the message.
        pub author: User,
        /// The content of the message.
        pub content: String,
        /// Initial message creation timestamp, calculated from its Id.
        pub timestamp: Timestamp,
        /// The timestamp of the last time the message was updated, if it was.
        pub edited_timestamp: Option<Timestamp>,
        // /// Array of users mentioned in the message.
        // pub mentions: Vec<User>,
        // /// Array of [`Role`]s' Ids mentioned in the message.
        // pub mention_roles: Vec<RoleId>,
        /// An vector of the files attached to a message.
        pub attachments: Vec<AAttachment>,
        /// Array of embeds sent with the message.
        // pub embeds: Vec<Embed>,
        /// Array of reactions performed on the message.
        // #[serde(default)]
        // pub reactions: Vec<MessageReaction>,
        /// Non-repeating number used for ensuring message order.
        // #[serde(default)]
        // pub nonce: Option<Nonce>,
        /// Indicator of whether the message is pinned.
        // pub pinned: bool,
        /// Indicator of the type of message this is, i.e. whether it is a regular message or a system
        /// message.
        #[serde(rename = "type")]
        pub kind: MessageType,
        // pub flags: Option<MessageFlags>,
        /// The message that was replied to using this message.
        // pub referenced_message: Option<MessageId>, // Changed to use id
        /// The thread that was started from this message, includes thread member object.
        pub thread: Option<GuildChannel>,
        // /// A generally increasing integer (there may be gaps or duplicates) that represents the
        // /// approximate position of the message in a thread, it can be used to estimate the relative
        // /// position of the message in a thread in company with total_message_sent on parent thread.
        // pub position: Option<u64>,
    }
    impl From<Message> for AMessage {
        fn from(v: Message) -> Self {
            AMessage {
                id: v.id,
                channel_id: v.channel_id,
                author: v.author,
                content: v.content,
                timestamp: v.timestamp,
                edited_timestamp: v.edited_timestamp,
                attachments: v
                    .attachments
                    .into_iter()
                    .map(|p| AAttachment::from(p))
                    .collect(),
                kind: v.kind,
                thread: v.thread,
            }
        }
    }

    #[derive(Serialize, Deserialize, Clone, Debug)]
    pub struct AGuildChannel {
        /// The unique Id of the channel.
        pub id: ChannelId,
        /// The Id of the parent category for a channel, or of the parent text channel for a thread.
        ///
        /// **Note**: This is only available for channels in a category and thread channels.
        pub parent_id: Option<ChannelId>,
        /// The Id of the guild the channel is located in.
        ///
        /// The original voice channel has an Id equal to the guild's Id, incremented by one.
        ///
        /// [`id`]: GuildChannel::id
        #[serde(default)]
        pub guild_id: GuildId,
        /// The type of the channel.
        #[serde(rename = "type")]
        pub kind: ChannelType,
        /// The Id of the user who created this channel
        ///
        /// **Note**: This is only available for threads and forum posts
        pub owner_id: Option<UserId>,
        /// The Id of the last message sent in the channel.
        ///
        /// **Note**: This is only available for text channels.
        pub last_message_id: Option<MessageId>,
        /// The timestamp of the time a pin was most recently made.
        ///
        /// **Note**: This is only available for text channels.
        pub last_pin_timestamp: Option<Timestamp>,
        /// The name of the channel.
        pub name: String,
        /// The position of the channel.
        ///
        /// The default text channel will _almost always_ have a position of `0`.
        #[serde(default)]
        pub position: u16,
        /// The topic of the channel.
        ///
        /// **Note**: This is only available for text, forum and stage channels.
        pub topic: Option<String>,
        #[serde(default)]
        pub nsfw: bool,
        /// The thread metadata.
        ///
        /// **Note**: This is only available on thread channels.
        pub thread_metadata: Option<ThreadMetadata>,
        /// The number of messages ever sent in a thread, it's similar to `message_count` on message
        /// creation, but will not decrement the number when a message is deleted.
        pub total_message_sent: Option<u64>,
        pub status: Option<String>,
    }
    impl From<GuildChannel> for AGuildChannel {
        fn from(v: GuildChannel) -> Self {
            Self {
                id: v.id,
                parent_id: v.parent_id,
                guild_id: v.guild_id,
                kind: v.kind,
                owner_id: v.owner_id,
                last_message_id: v.last_message_id,
                last_pin_timestamp: v.last_pin_timestamp,
                name: v.name,
                position: v.position,
                topic: v.topic,
                nsfw: v.nsfw,
                thread_metadata: v.thread_metadata,
                total_message_sent: v.total_message_sent,
                status: v.status,
            }
        }
    }
}
