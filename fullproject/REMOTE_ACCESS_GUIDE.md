# Remote HDFS Access Guide — 5G KPI Pipeline

## Project Overview
This project processes 5G network PCAP files (~12 GB, 12 shards) stored in HDFS at:
```
/5g_kpi/sharded/URLLC/    → naver5g3-10M_part001.pcap to part012.pcap
/5g_kpi/sharded/eMBB/     → (similar shards)
/5g_kpi/sharded/mMTC/     → (similar shards)
```
HDFS NameNode runs inside Docker at `hdfs://namenode:8020`.

---

## METHOD 1: ngrok Tunnel (EASIEST — Recommended)

### What is ngrok?
ngrok creates a secure public URL that tunnels traffic to your local machine. No router config, no firewall changes needed. Works instantly across any network.

### Step-by-Step (Host Machine — Your PC)

#### 1. Install ngrok
```powershell
# Option A: Download from https://ngrok.com/download (Windows ZIP)
# Option B: Using winget
winget install ngrok
```

#### 2. Sign up & authenticate
- Go to https://ngrok.com and create a free account
- Copy your auth token from the dashboard
```powershell
ngrok config add-authtoken YOUR_AUTH_TOKEN
```

#### 3. Check which port HDFS NameNode WebUI is mapped to on your host
```powershell
docker ps --format "table {{.Names}}\t{{.Ports}}" | findstr namenode
```
Look for a mapping like `0.0.0.0:9870->9870/tcp`. If port 9870 is mapped, proceed. If not:
```powershell
# Stop and re-run namenode with port mapping (adjust your docker-compose or run command):
# Ensure these ports are exposed: 9870 (WebUI + WebHDFS), 8020 (RPC)
```

#### 4. Expose HDFS NameNode WebUI via ngrok
```powershell
ngrok http 9870
```
You'll see output like:
```
Forwarding   https://abc123.ngrok-free.app -> http://localhost:9870
```

#### 5. Share the ngrok URL with your teammate
Send them the URL: `https://abc123.ngrok-free.app`

### How Teammate Accesses Files (Remote PC)

#### Browse Files (Browser)
```
https://abc123.ngrok-free.app/explorer.html#/5g_kpi/sharded/URLLC
```

#### Download Files via WebHDFS REST API (Terminal/curl)
```bash
# List all files in URLLC directory
curl -L "https://abc123.ngrok-free.app/webhdfs/v1/5g_kpi/sharded/URLLC?op=LISTSTATUS"

# Download a specific PCAP file
curl -L -o naver5g3-10M_part001.pcap \
  "https://abc123.ngrok-free.app/webhdfs/v1/5g_kpi/sharded/URLLC/naver5g3-10M_part001.pcap?op=OPEN"
```

#### Download Files via Python
```python
import requests

NGROK_URL = "https://abc123.ngrok-free.app"  # Replace with actual URL

# List files
resp = requests.get(f"{NGROK_URL}/webhdfs/v1/5g_kpi/sharded/URLLC?op=LISTSTATUS", 
                     allow_redirects=True)
files = resp.json()["FileStatuses"]["FileStatus"]
for f in files:
    print(f["pathSuffix"], f"{f['length']/(1024**3):.2f} GB")

# Download a file
file_name = "naver5g3-10M_part001.pcap"
resp = requests.get(
    f"{NGROK_URL}/webhdfs/v1/5g_kpi/sharded/URLLC/{file_name}?op=OPEN",
    allow_redirects=True, stream=True
)
with open(file_name, "wb") as f:
    for chunk in resp.iter_content(chunk_size=8192):
        f.write(chunk)
print(f"Downloaded {file_name}")
```

#### Download ALL Files (Bash Script)
```bash
#!/bin/bash
NGROK_URL="https://abc123.ngrok-free.app"

for SLICE in URLLC eMBB mMTC; do
  mkdir -p $SLICE
  # Get file list
  FILES=$(curl -s "$NGROK_URL/webhdfs/v1/5g_kpi/sharded/$SLICE?op=LISTSTATUS" \
    | python3 -c "import sys,json; [print(f['pathSuffix']) for f in json.load(sys.stdin)['FileStatuses']['FileStatus']]")
  
  for FILE in $FILES; do
    echo "Downloading $SLICE/$FILE ..."
    curl -L -o "$SLICE/$FILE" \
      "$NGROK_URL/webhdfs/v1/5g_kpi/sharded/$SLICE/$FILE?op=OPEN"
  done
done
echo "All files downloaded!"
```

### ⚠ Important Notes for ngrok
- Free tier: URL changes every time you restart ngrok. Share the new URL each session.
- Free tier has bandwidth limits (~1 GB/month). For 12 GB of data, consider a paid plan ($8/month) or use Method 2.
- The ngrok tunnel must stay running on your machine while teammates are accessing files.

---

## METHOD 2: Tailscale VPN (BEST for Ongoing Collaboration)

### What is Tailscale?
Tailscale creates a private mesh VPN between your machines. Both machines get a static private IP and can communicate as if on the same LAN. Free for personal use (up to 100 devices).

### Setup (Both Machines)

#### 1. Install Tailscale
- **Your PC (Windows):** Download from https://tailscale.com/download/windows
- **Teammate's PC:** Download for their OS from https://tailscale.com/download

#### 2. Sign in with the same account (or invite teammate)
Both of you sign in at https://login.tailscale.com using the same Google/Microsoft/GitHub account, OR:
- You create a Tailscale network (tailnet)
- Go to https://login.tailscale.com/admin/invite → invite your teammate's email

#### 3. Get your Tailscale IP
```powershell
tailscale ip -4
# Example output: 100.64.0.1
```

#### 4. Ensure HDFS ports are accessible
Your Docker containers must map ports to your host:
```
NameNode WebUI:  9870  (for browsing + WebHDFS API)
NameNode RPC:    8020  (for hadoop fs commands)
DataNode:        9864  (for data transfer)
```

### How Teammate Accesses Files

#### Browse via Browser
```
http://YOUR_TAILSCALE_IP:9870/explorer.html#/5g_kpi/sharded/URLLC
```

#### Use WebHDFS REST API
```bash
# List files
curl "http://YOUR_TAILSCALE_IP:9870/webhdfs/v1/5g_kpi/sharded/URLLC?op=LISTSTATUS"

# Download file
curl -L -o naver5g3-10M_part001.pcap \
  "http://YOUR_TAILSCALE_IP:9870/webhdfs/v1/5g_kpi/sharded/URLLC/naver5g3-10M_part001.pcap?op=OPEN"
```

#### Use hadoop CLI directly (if teammate has Hadoop installed)
Teammate adds to their `core-site.xml`:
```xml
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://YOUR_TAILSCALE_IP:8020</value>
  </property>
</configuration>
```
Then:
```bash
hadoop fs -ls /5g_kpi/sharded/URLLC/
hadoop fs -get /5g_kpi/sharded/URLLC/naver5g3-10M_part001.pcap ./
```

---

## METHOD 3: HDFS Configuration Changes (Required for Both Methods)

Your HDFS NameNode and DataNode must be configured to accept external connections. Apply these settings:

### On Your Machine — Update HDFS Config

#### hdfs-site.xml (add/update these properties)
```xml
<configuration>
  <!-- Allow WebHDFS REST API -->
  <property>
    <name>dfs.webhdfs.enabled</name>
    <value>true</value>
  </property>

  <!-- Allow connections from any host -->
  <property>
    <name>dfs.namenode.http-address</name>
    <value>0.0.0.0:9870</value>
  </property>

  <!-- DataNode should return hostname accessible to remote client -->
  <property>
    <name>dfs.client.use.datanode.hostname</name>
    <value>true</value>
  </property>

  <property>
    <name>dfs.datanode.use.datanode.hostname</name>
    <value>true</value>
  </property>

  <!-- Disable permissions for easy access (development only!) -->
  <property>
    <name>dfs.permissions.enabled</name>
    <value>false</value>
  </property>
</configuration>
```

#### core-site.xml (add this for proxy user access)
```xml
<configuration>
  <property>
    <name>hadoop.proxyuser.spark.hosts</name>
    <value>*</value>
  </property>
  <property>
    <name>hadoop.proxyuser.spark.groups</name>
    <value>*</value>
  </property>
</configuration>
```

### If Using Docker — Apply Config via docker exec
```powershell
# Enter namenode container
docker exec -it namenode bash

# Edit hdfs-site.xml (location depends on your Hadoop setup)
# Typically at /opt/hadoop/etc/hadoop/hdfs-site.xml or /etc/hadoop/hdfs-site.xml

# After editing, restart HDFS services:
hdfs namenode -format  # ONLY if first time
stop-dfs.sh && start-dfs.sh
```

---

## METHOD 4: DataNode Redirect Fix (CRITICAL)

When a remote client downloads a file via WebHDFS, the NameNode redirects to a DataNode. If the DataNode returns an internal Docker hostname (like `datanode:9864`), the remote client can't resolve it.

### Fix: Map DataNode hostname on Teammate's Machine

#### On Teammate's PC — Add to hosts file
**Windows:** Edit `C:\Windows\System32\drivers\etc\hosts`
**Linux/Mac:** Edit `/etc/hosts`

Add:
```
YOUR_PUBLIC_IP_OR_TAILSCALE_IP   datanode
YOUR_PUBLIC_IP_OR_TAILSCALE_IP   namenode
```

For ngrok, this redirect issue is handled automatically since ngrok proxies the full connection.

---

## Quick Reference — Summary Table

| Method        | Setup Time | Bandwidth   | Persistent | Best For              |
|---------------|-----------|-------------|------------|----------------------|
| **ngrok**     | 5 min     | Limited (free) | No (URL changes) | Quick one-time sharing |
| **Tailscale** | 10 min    | Unlimited   | Yes (static IP)  | Ongoing collaboration  |

---

## Verification — Test Remote Access

### From Teammate's Machine (after setup)
```bash
# 1. Test connectivity
curl -s "BASE_URL/webhdfs/v1/?op=LISTSTATUS" | head -50

# 2. Check URLLC files exist
curl -s "BASE_URL/webhdfs/v1/5g_kpi/sharded/URLLC?op=LISTSTATUS" | python3 -m json.tool

# 3. Download test (small file — part012 is 31.49 MB)
curl -L -o test_part012.pcap \
  "BASE_URL/webhdfs/v1/5g_kpi/sharded/URLLC/naver5g3-10M_part012.pcap?op=OPEN"
ls -lh test_part012.pcap
# Expected: ~31.49 MB
```
Replace `BASE_URL` with your ngrok URL or `http://TAILSCALE_IP:9870`.

---

## Files Available in HDFS

| # | File Name                    | Size    | Path                                   |
|---|------------------------------|---------|----------------------------------------|
| 1 | naver5g3-10M_part001.pcap   | 1 GB    | /5g_kpi/sharded/URLLC/                |
| 2 | naver5g3-10M_part002.pcap   | 1 GB    | /5g_kpi/sharded/URLLC/                |
| 3 | naver5g3-10M_part003.pcap   | 1 GB    | /5g_kpi/sharded/URLLC/                |
| 4 | naver5g3-10M_part004.pcap   | 1 GB    | /5g_kpi/sharded/URLLC/                |
| 5 | naver5g3-10M_part005.pcap   | 1 GB    | /5g_kpi/sharded/URLLC/                |
| 6 | naver5g3-10M_part006.pcap   | 1 GB    | /5g_kpi/sharded/URLLC/                |
| 7 | naver5g3-10M_part007.pcap   | 1 GB    | /5g_kpi/sharded/URLLC/                |
| 8 | naver5g3-10M_part008.pcap   | 1 GB    | /5g_kpi/sharded/URLLC/                |
| 9 | naver5g3-10M_part009.pcap   | 1 GB    | /5g_kpi/sharded/URLLC/                |
| 10| naver5g3-10M_part010.pcap   | 1 GB    | /5g_kpi/sharded/URLLC/                |
| 11| naver5g3-10M_part011.pcap   | 1 GB    | /5g_kpi/sharded/URLLC/                |
| 12| naver5g3-10M_part012.pcap   | 31.49 MB| /5g_kpi/sharded/URLLC/                |

**Total: ~11.03 GB** | Replication factor: 3 | Block size: 128 MB

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| `Connection refused` on port 9870 | Ensure Docker maps port: `docker ps` → check 9870 mapping |
| `Could not resolve hostname datanode` | Add hosts file entry mapping datanode to your IP |
| ngrok URL stopped working | Restart ngrok, share new URL |
| `Permission denied` on file download | Set `dfs.permissions.enabled=false` in hdfs-site.xml |
| Slow download speed | Normal for 1 GB files over internet; use Tailscale for LAN-like speed |
| `403 Forbidden` from WebHDFS | Add proxy user config in core-site.xml (see Method 3) |
| Teammate can browse but can't download | DataNode redirect issue — apply Method 4 fix |
