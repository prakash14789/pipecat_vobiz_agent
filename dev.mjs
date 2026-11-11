import { spawn } from 'child_process';
import path from 'path';
import fs from 'fs';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// Detect python executable from venv or system
function getPythonExecutable() {
  const winVenv = path.join(__dirname, '.venv', 'Scripts', 'python.exe');
  const posixVenv = path.join(__dirname, '.venv', 'bin', 'python');

  if (process.platform === 'win32' && fs.existsSync(winVenv)) {
    return winVenv;
  }
  if (process.platform !== 'win32' && fs.existsSync(posixVenv)) {
    return posixVenv;
  }
  return process.platform === 'win32' ? 'python' : 'python3';
}

// Update PUBLIC_URL in .env file with active tunnel URL
function updateEnvPublicUrl(url) {
  const envPath = path.join(__dirname, '.env');
  if (fs.existsSync(envPath)) {
    let envContent = fs.readFileSync(envPath, 'utf8');
    if (envContent.includes('PUBLIC_URL=')) {
      envContent = envContent.replace(/^PUBLIC_URL=.*$/m, `PUBLIC_URL=${url}`);
    } else {
      envContent += `\nPUBLIC_URL=${url}\n`;
    }
    fs.writeFileSync(envPath, envContent, 'utf8');
    console.log(`  Updated PUBLIC_URL: ${url}`);
  }
}

const pythonExe = getPythonExecutable();
console.log('='.repeat(60));
console.log('  Starting Pipecat Vobiz Voice AI Suite');
console.log(`  Backend Python:  ${pythonExe}`);
console.log('  Backend Server:  http://127.0.0.1:8000');
console.log('  Frontend Vite:   http://localhost:3000');
console.log('='.repeat(60));

// Spawn cloudflared tunnel if binary exists
const localBin = path.join(__dirname, process.platform === 'win32' ? 'cloudflared.exe' : 'cloudflared');
const cloudflaredExe = fs.existsSync(localBin) ? localBin : (fs.existsSync(path.join(__dirname, 'cloudflared.exe')) ? path.join(__dirname, 'cloudflared.exe') : null);
let tunnelProcess = null;

if (cloudflaredExe && fs.existsSync(cloudflaredExe)) {
  tunnelProcess = spawn(cloudflaredExe, ['tunnel', '--url', 'http://127.0.0.1:8000'], {
    cwd: __dirname,
    shell: false,
  });

  const onTunnelOutput = (data) => {
    const text = data.toString();
    const match = text.match(/https:\/\/[a-z0-9-]+\.trycloudflare\.com/i);
    if (match) {
      updateEnvPublicUrl(match[0]);
    }
  };

  tunnelProcess.stdout.on('data', onTunnelOutput);
  tunnelProcess.stderr.on('data', onTunnelOutput);
}

// Spawn Python backend server
const backend = spawn(pythonExe, ['server.py'], {
  cwd: __dirname,
  stdio: 'inherit',
  shell: false,
});

// Spawn Vite frontend dev server
const npmCmd = process.platform === 'win32' ? 'npm.cmd' : 'npm';
const frontend = spawn(npmCmd, ['run', 'dev'], {
  cwd: path.join(__dirname, 'frontend'),
  stdio: 'inherit',
  shell: process.platform === 'win32',
});

// Terminate child processes on exit
function shutdown() {
  console.log('\nShutting down servers...');
  try {
    if (process.platform === 'win32') {
      if (backend?.pid) spawn('taskkill', ['/pid', backend.pid.toString(), '/f', '/t'], { shell: true });
      if (frontend?.pid) spawn('taskkill', ['/pid', frontend.pid.toString(), '/f', '/t'], { shell: true });
      if (tunnelProcess?.pid) spawn('taskkill', ['/pid', tunnelProcess.pid.toString(), '/f', '/t'], { shell: true });
    } else {
      backend?.kill('SIGTERM');
      frontend?.kill('SIGTERM');
      tunnelProcess?.kill('SIGTERM');
    }
  } catch {
    // Process cleanup completed
  }
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);

backend.on('exit', (code) => {
  if (code !== 0 && code !== null) {
    console.error(`Backend exited with code ${code}`);
    shutdown();
  }
});

frontend.on('exit', (code) => {
  if (code !== 0 && code !== null) {
    console.error(`Frontend exited with code ${code}`);
    shutdown();
  }
});
