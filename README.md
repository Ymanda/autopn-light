## AutoPN — sanitized release candidate

This repository now ships only the two flows that proved reliable:

1. Convert WhatsApp exports into an email-like plain text (`parse_whatsapp.py`)
2. Analyse those emails with GPT and produce highlighted HTML + CSV digests (`sophism_report.py`)
3. BONUS :Payment monitoor and automatic reminders by email.

---

### 1. Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Set your OpenAI key once (either via `.env` or the shell):

```bash
echo "OPENAI_API_KEY=sk-..." > .env
```

---

### 2. Configure relations

1. Copy the example config and edit it:
   ```bash
   cp config/autopn.example.yaml config/autopn.yaml
   ```
2. For each relation define:
   - `emails`: the addresses that belong to them
   - `email_archives.dir`: where your `emails_YYYY.txt` files live (e.g. `data/relations/example/emails`)
   - optional `whatsapp.chat_export` + `whatsapp.email_output_dir`
   - `reports.*` paths for HTML + CSV outputs (defaults to subfolders inside `email_archives.dir`)
   - any `context_history` you want injected into the LLM prompt

The repo keeps `data/` empty; drop your exports there locally.

---

### 3. Convert WhatsApp chats (optional)

```bash
python parse_whatsapp.py --relation example
```

Arguments:

- `--chat-file` to override the export path defined in the config
- `--output-dir` to store the generated `whatsapp_<year>.txt`

The script deduplicates messages per year and writes the AutoPN email format.

---

### 4. Generate a sophism report

```bash
python sophism_report.py \
  --relation example \
  --years 2023-2024 \
  --taxonomy-mode hint
```

Key flags:

- `--year` / `--years all|2022-2024|2021,2024`
- `--normalized` to read `emails_YYYY_NORMALIZED.txt`
- `--max` to cap the number of analysed messages (handy for dry-runs)
- `--clear-light` / `--clear-events` to reset CSV outputs
- `--taxonomy-file` if you maintain your own sophism list

Outputs per relation:

- HTML reports under `reports.html_dir` (default: `<emails_dir>/sophismes`)
- A “rich” CSV (`reports.csv_events`) with one line per detected event
- A light CSV (`reports.csv_light`) that you can open in Excel / Airtable

---

### Repository layout

```
autopn/                Helper modules (config + event sink)
config/autopn.example.yaml   Ready-to-copy template for your settings
data/                  Empty placeholder; drop your private exports here
parse_whatsapp.py      WhatsApp → AutoPN text converter
sophism_report.py      GPT-powered analyser + HTML/CSV renderer
sophismes.yaml         Default taxonomy
requirements.txt       Minimal dependencies (openai, dotenv, PyYAML)
```

---

### Licensing

The project is published under the MIT License (see `LICENSE`). You are free to adapt it for private use; contributions that keep it privacy-friendly and easy to self-host are welcome.

---

### Why AutoPN exists

AutoPN is a private aide-mémoire for emotionally charged or high-stakes conversations. By converting WhatsApp/email threads into a neutral timeline, then asking the LLM to flag sophisms, it becomes easier to:

- spot recurring tension triggers (e.g., guilt-tripping, topic switching, straw-man arguments)
- document what each party openly asked for versus what was implied or held back
- surface “hidden matters” (money, promises, authority) that often sit behind long fights

Typical use cases:

- **Family disputes**: inheritance, care duties, financial support, co-parenting logistics.
- **Contracts & business deals**: when one party rewrites history mid-negotiation.
- **Workplace power plays**: tyrannic managers, unpaid overtime, “friendly” requests that need a paper trail.
- **Your own experiments**: e.g., documenting harassment patterns in online communities or coaching someone on assertive replies.

Think of the reports as a friendly paralegal: it doesn’t replace a lawyer or therapist, but it gives you a structured view of who said what, why, and how credible it looked.

---

### Security & credentials in plain language

- **Email passwords**: never hard-code them. Use app-specific passwords when possible (Gmail, Outlook, Proton all support this). Store them in `.env` (ignored by git) or export them in your shell session. Rotate them after each audit.
- **IMAP vs POP**: IMAP keeps the mailbox in sync (including sent mail); POP usually downloads only inbox items and may delete copies server-side. If you need both incoming and sent conversations, IMAP access (or Gmail “All Mail”) is the safer choice. If you only have POP, export the “Sent” folder separately and drop it into the same `emails_YYYY.txt` structure.
- **OpenAI API keys / passkeys**: treat them like credit cards. Create a dedicated key for AutoPN, restrict it to the `gpt-4o-mini` family, set a spending cap, and revoke it if the machine or repo ever feels compromised. The key lives in `.env` (`OPENAI_API_KEY=…`) or an environment variable; never commit it.

---

### End-to-end flow in everyday terms

1. **Capture**: export a chat/email thread (WhatsApp text file, Gmail takeout, Outlook PST converted to text) and drop it under `data/…`.
2. **Normalize**: run `parse_whatsapp.py` (for chats) or merge emails manually into `emails_YYYY.txt` blocks.
3. **Analyse**: run `sophism_report.py` for the year(s) that matter.
4. **Review**: open the HTML report in a browser, filter the CSV for hidden topics, and copy any excerpts you want to discuss with professionals (lawyer, mediator, coach).
5. **Decide**: use the findings to write calmer replies, design boundaries, draft clauses, or simply archive a clean record for later.

AutoPN is best used when memory, stress, or deliberate gaslighting makes it hard to argue facts. The tool keeps you grounded in evidence and lets you focus on next steps instead of endless “he said / she said”.
