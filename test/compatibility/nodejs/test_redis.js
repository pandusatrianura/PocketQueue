const net = require('net');
const { spawn, execSync } = require('child_process');
const path = require('path');
const Redis = require('ioredis');

function findFreePort() {
  return new Promise((resolve, reject) => {
    const srv = net.createServer();
    srv.listen(0, '127.0.0.1', () => {
      const port = srv.address().port;
      srv.close(() => resolve(port));
    });
    srv.on('error', reject);
  });
}

async function run() {
  const redisPort = await findFreePort();
  const kafkaPort = await findFreePort();

  const rootDir = path.resolve(__dirname, '..', '..', '..');
  const binName = process.platform === 'win32' ? 'pocketqueue_node_compat.exe' : 'pocketqueue_node_compat';
  const binPath = path.join(rootDir, binName);

  console.log(`[*] Building test binary at ${binPath}...`);
  execSync(`go build -o "${binPath}" ./cmd/pocketqueue`, { cwd: rootDir });

  console.log(`[*] Spawning PocketQueue (Redis: ${redisPort}, Kafka: ${kafkaPort})...`);
  const proc = spawn(binPath, [
    '--headless',
    '--host', '127.0.0.1',
    '--redis-port', String(redisPort),
    '--kafka-port', String(kafkaPort),
  ]);

  let client;
  try {
    // Wait for server ready
    let connected = false;
    for (let i = 0; i < 30; i++) {
      try {
        client = new Redis({
          host: '127.0.0.1',
          port: redisPort,
          maxRetriesPerRequest: 1,
          lazyConnect: true,
          retryStrategy: () => null,
        });
        await client.connect();
        const pong = await client.ping();
        if (pong === 'PONG') {
          connected = true;
          break;
        }
      } catch (e) {
        if (client) {
          try { client.disconnect(); } catch (err) {}
        }
        await new Promise((r) => setTimeout(r, 100));
      }
    }

    if (!connected) {
      throw new Error('Failed to connect to PocketQueue Redis server');
    }

    console.log('[*] 1. PING -> PASS');

    console.log('[*] 2. Testing XADD...');
    const e1 = await client.xadd('node-stream', '*', 'device', 'pi-4', 'status', 'online');
    if (e1 !== '0-0') throw new Error(`Unexpected entry ID: ${e1}`);
    console.log(`    -> PASS (ID: ${e1})`);

    console.log('[*] 3. Testing XLEN...');
    const len = await client.xlen('node-stream');
    if (len !== 1) throw new Error(`Expected len 1, got ${len}`);
    console.log(`    -> PASS (Len: ${len})`);

    console.log('[*] 4. Testing XRANGE...');
    const range = await client.xrange('node-stream', '-', '+');
    if (range.length !== 1 || range[0][0] !== '0-0') {
      throw new Error(`Unexpected xrange result: ${JSON.stringify(range)}`);
    }
    console.log(`    -> PASS (Entries: ${range.length})`);

    console.log('[*] 5. Testing XGROUP CREATE...');
    const grp = await client.xgroup('CREATE', 'node-stream', 'workers', '$', 'MKSTREAM');
    if (grp !== 'OK') throw new Error(`Expected OK from XGROUP CREATE, got ${grp}`);
    console.log(`    -> PASS`);

    console.log('[*] 6. Testing XADD for consumer group...');
    const e2 = await client.xadd('node-stream', '*', 'device', 'pi-5', 'status', 'reboot');
    if (e2 !== '1-0') throw new Error(`Unexpected entry ID: ${e2}`);
    console.log(`    -> PASS (ID: ${e2})`);

    console.log('[*] 7. Testing XREADGROUP...');
    const rg = await client.xreadgroup('GROUP', 'workers', 'c-1', 'COUNT', 10, 'STREAMS', 'node-stream', '>');
    if (!rg || rg.length === 0 || rg[0][0] !== 'node-stream') {
      throw new Error(`Unexpected xreadgroup result: ${JSON.stringify(rg)}`);
    }
    console.log(`    -> PASS (ReadGroup streams: ${rg.length})`);

    console.log('[*] 8. Testing XACK...');
    const ack = await client.xack('node-stream', 'workers', '1-0');
    if (ack !== 1) throw new Error(`Expected 1 ack, got ${ack}`);
    console.log(`    -> PASS (Ack: ${ack})`);

    console.log('[*] 9. Testing XREVRANGE...');
    const rev = await client.xrevrange('node-stream', '+', '-', 'COUNT', 1);
    if (!rev || rev.length !== 1 || rev[0][0] !== '1-0') {
      throw new Error(`Unexpected xrevrange result: ${JSON.stringify(rev)}`);
    }
    console.log(`    -> PASS (RevRange ID: ${rev[0][0]})`);

    console.log('[*] 10. Testing XDEL...');
    const del = await client.xdel('node-stream', '0-0');
    if (del !== 1) throw new Error(`Expected 1 deleted, got ${del}`);
    console.log(`    -> PASS (Deleted: ${del})`);

    console.log('\n[SUCCESS] All Node.js ioredis compatibility tests passed!');
  } finally {
    if (client) {
      try { client.disconnect(); } catch (e) {}
    }
    proc.kill();
    const fs = require('fs');
    if (fs.existsSync(binPath)) {
      try {
        fs.unlinkSync(binPath);
      } catch (e) {}
    }
  }
}

run().catch((err) => {
  console.error('[FAIL]', err);
  process.exit(1);
});
