## Basic usage
- Install chrome
- Install uv: [https://docs.astral.sh/uv/getting-started/installation](https://docs.astral.sh/uv/getting-started/installation)
- Install required dependencies: run `uv sync --no-dev`
- Edit `config.yaml`
- Run `uv run python app.py -f config.yaml`

## Send notification
- Install dependencies: run `uv sync --no-dev --extra notify`
- Edit `apprise.yaml`: https://github.com/caronc/apprise#supported-notifications
- Modify `notify` in `config.yaml`

## Chrome setup
### Use custom profile
- Create chrome profile: run `chrome --user-data-dir=<user_data_dir>`
- Install extensions
  - Chrome Webstore (optional): [https://github.com/NeverDecaf/chromium-web-store?tab=readme-ov-file#read-this-first](https://github.com/NeverDecaf/chromium-web-store?tab=readme-ov-file#read-this-first) 
  - uBlock Origin: [https://chromewebstore.google.com/detail/cjpalhdlnbpafiamejdnhcphjbkeiagm](https://chromewebstore.google.com/detail/cjpalhdlnbpafiamejdnhcphjbkeiagm)
  - User-Agent Switcher and Manager: [https://chromewebstore.google.com/detail/bhchdcejhohfmigjafbampogmaanbfkg](https://chromewebstore.google.com/detail/bhchdcejhohfmigjafbampogmaanbfkg)
- Edit `config.yaml`
  - Modify `user_data_dir`, every website need one **unique** user_data_dir
  - Modify `remote_debugging_port`, different port for each profile
  - Modify `incognito: false`

### uBlock Origin filters
```
facebook.com##+js(trusted-click-element, body > div[id^="mount"] #scrollview ~ div div[role="button"]:has(> div[data-visualcompletion="ignore"]))
facebook.com##div[id^="mount"] div:not([id]):not([class]):not([style]) > div[data-nosnippet]
facebook.com##+js(aeld, scroll)
facebook.com##body > div[class*="__fb-light-mode"]
```
source: [https://www.reddit.com/r/uBlockOrigin/comments/1g6ptm4/comment/ltxi5ti](https://www.reddit.com/r/uBlockOrigin/comments/1g6ptm4/comment/ltxi5ti)

### User-Agent Switcher and Manager
To override user-agent and window.navigator property, use these options: 
- userAgent: `Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36` (this should match chrome major version)
- platform: `Win32`
- product: `Gecko`
- vendor: `Google Inc.`
