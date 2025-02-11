import { ipcMain } from 'electron';
import { configDir } from './config.js';

const path = await import('path');
const fs = await import('fs');

const LevelDisabled = 0;
const LevelDefault = 1;
const LevelAll = 2;

let level = -1;

export function getNotificationLevel() {
    if (level === -1) {
        try {
            const cfg = fs.readFileSync(path.join(configDir(), 'notifications.json'));
            return JSON.parse(cfg).level;
        } catch (e) {
            level = LevelDefault;
        }
    }

    return level;
}

export function setNotificationLevel(l) {
    level = l;
    if (level < LevelDisabled || level > LevelAll) {
        level = LevelDefault;
    }

    fs.writeFileSync(path.join(configDir(), 'notifications.json'), JSON.stringify({ level: l }));

    ipcMain.emit('notification-config-updated');
}
