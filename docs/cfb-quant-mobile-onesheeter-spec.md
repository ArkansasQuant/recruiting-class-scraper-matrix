# CFB Quant — mobile density, One Sheeter export, Players position tabs

Build spec, 2026-08-14. Repo `AustinAlexander01/cfb-quant` (private — apply via Claude Code).
Checked `aq_conventions` (ids 1, 2, 3, 5, 25, 35, 36, 46, 48, 56, 68, 69, 70).

---

## 0. Deployment gate — read before building item 5

Verified in `iyrsrlrrrjnzsxguciph` on 2026-08-14:

| Object | RLS | `authenticated` SELECT | `anon` SELECT |
|---|---|---|---|
| `v3_pv` | on | **NO** | no |
| `v3_board` | on | yes (grant) but **no policy exists** | no |
| `players`, `teams`, `pff_defense`, `pff_blocking` | on | yes + policy | no |

Public schema totals: 248 objects, `authenticated` SELECT on 226, `anon` on **0**.

Consequences:

1. Any browser query against `v3_pv` returns 404 / empty — same failure mode as the Coordinator tab. The position tabs must read a **new view with an explicit grant + policy**, not `v3_pv` directly.
2. `v3_board` has RLS enabled with **no policy**, so a browser read returns zero rows regardless of the grant. If the Watchlist tab works today it is going through the service key server-side. Confirm which before reusing that path.
3. Every new object follows the existing naming pattern: `authenticated_read_<name>`, PERMISSIVE, SELECT, `using (true)`.

---

## 1. Teams search — recover vertical space above the keyboard

**Diagnosis.** Not the ordering — the problem is how little of the screen is left for results. Measured off the 3x screenshot, roughly: page title block ~40 CSS px, account/sign-out row ~30, nav row ~24, search box + margins ~55, conference section header ~50, and each team card ~50 with its own margin. Keyboard top leaves about 210 CSS px of results area. One header plus one card fills it exactly, which is why only Florida State shows.

Everything below is a vertical-space fix. Attack it in this order — the first two recover the most and are the least invasive.

**(a) Collapse the page chrome while the search input is focused.** The title, the email + SIGN OUT row and the nav are ~95 CSS px of chrome that nothing in the search flow needs. On focus, animate the title and account row to zero height and leave a compact sticky nav. Restore on blur. Biggest single win, roughly doubles the results area on its own.

**(b) Conference headers from block to inline band.** 11px uppercase on an 18px band with a hairline rule, no margin above or below — not a 50px section break. Grouping stays; it just stops costing a row per conference.

**(c) Shorter rows.** Card → row: 34px tall, 20px logo, name at 15px, one hairline divider, no border-radius, no card background, no per-item margin. From ~50px to 34px each.

**(d) Two columns while a query is active.** `grid-template-columns: repeat(2, 1fr)` with a 1px gutter above 360px viewport width, single column below. Doubles results per vertical band. Keep single-column for the empty-query browse state where reading down the conference matters.

Combined, the same viewport goes from one visible result to roughly ten.

**(e) Optional, secondary.** Rank matches rather than alpha-sorting the filtered set — prefix match, then word-prefix (`State` → Florida State, Ohio State), then abbreviation, then substring. With (a)–(d) shipped every match is on screen anyway, so this is polish, not a fix.

- Scroll container sized to the space the keyboard leaves:

```js
// keep the list above the iOS keyboard
useEffect(() => {
  const vv = window.visualViewport;
  if (!vv) return;
  const fit = () => {
    const top = listRef.current?.getBoundingClientRect().top ?? 0;
    setMaxH(Math.max(160, vv.height - top + vv.offsetTop - 12));
  };
  fit();
  vv.addEventListener('resize', fit);
  vv.addEventListener('scroll', fit);
  return () => { vv.removeEventListener('resize', fit); vv.removeEventListener('scroll', fit); };
}, []);
```

  Then `style={{ maxHeight: maxH, overflowY: 'auto', WebkitOverflowScrolling: 'touch' }}`. Six to eight rows land in view instead of one.
- Conference label: pull from `team_tier_seasons` for the current season, not `teams.conference` — conference is season-keyed and realignment-aware (convention #3).
- Optional, not required: alias map for `Ole Miss`, `Miss St`, `FIU`, `UCF`, `App State`, `Pitt`.

---

## 2. HC / OC / DC column width

Three columns each sized to the longest full name is what blows the table past the viewport.

- `table-layout: fixed` on the table. Without it the browser ignores your widths and re-fits to content.
- Below 480px, render **first initial + surname**: `B. Petrino`, not `Bobby Petrino`. Roughly 11ch instead of 18–22ch. Keep the full name in `title` and in the export render.

```js
const shortName = (n) => {
  if (!n) return '—';
  const parts = n.trim().split(/\s+/)
    .filter(p => !/^(jr\.?|sr\.?|ii|iii|iv|v)$/i.test(p));
  const last = parts.at(-1);
  return parts.length > 1 ? `${parts[0][0]}. ${last}` : last;
};
```

- Cell rules for the three coach columns: `max-width: 11ch; white-space: nowrap; overflow: hidden; text-overflow: ellipsis;`
- Mobile table defaults: `font-size: 13px`, cell padding `4px 6px`, `font-variant-numeric: tabular-nums` on every numeric column.
- Combined effect frees roughly 30–35% of table width. If that still isn't enough, the next lever is stacking HC/OC/DC into a single two-line cell under 380px — do that only if measured, don't build it speculatively.
- Do **not** apply any of this to the export render. The One Sheeter renders at 1080px with full names (see item 3).

---

## 3. One Sheeter → high-res X image on iPhone

Four independent things break image export on iOS. Fix all four; any one of them alone produces a blank, black, or silently-nothing-happens result.

**(a) The download never fires.** `<a download>` with a blob URL is unreliable on iOS Safari. The working path is the Web Share API:

```js
const file = new File([blob], `${team}-${tab}-${season}.png`, { type: 'image/png' });
if (navigator.canShare?.({ files: [file] })) {
  await navigator.share({ files: [file] });      // iOS: Save Image / post to X
} else {
  const url = URL.createObjectURL(blob);          // desktop
  Object.assign(document.createElement('a'), { href: url, download: file.name }).click();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
```

`navigator.share` must be called inside the user-activation window. An `await`-ed render before it will consume that window and the share sheet silently won't open. **Pre-render the blob when the sheet opens** (idle) and have the button share an already-built file. If it isn't ready, show `Preparing…` and surface a second explicit button — the second tap is a fresh gesture.

Always ship the belt-and-braces fallback too: render the PNG into a modal `<img>` so the user can long-press → Save Image. That path works on every iOS version.

**(b) You are capturing the mobile DOM.** "Full scope of the table and nothing else" cannot come from the responsive table — that table is deliberately truncated per items 1–2. Build a separate `<OneSheet>` component and mount it offscreen at fixed width:

```jsx
<div ref={exportRef} style={{
  position: 'fixed', left: -10000, top: 0, width: 1080,
  background: '#fff', color: '#111'
}}>
  <OneSheet ... />   {/* header, watermark, norm row, all rows, footnote. Nothing else. */}
</div>
```

Fixed 1080px logical width means media queries never fire on it and full coach names render.

**(c) iOS canvas area cap.** Mobile Safari zeroes out canvases past roughly 16.7M pixels — the usual cause of a blank export at `pixelRatio: 3`. Compute the ratio instead of hardcoding:

```js
const { width: w, height: h } = node.getBoundingClientRect();
const MAX_PX = 16_000_000;
const scale = Math.max(1.5, Math.min(3, Math.sqrt(MAX_PX / (w * h))));
```

At 1080 × 2400 that yields ~2.5, i.e. ~2700px wide. Plenty for X. Long tables degrade gracefully instead of failing.

**(d) The capture library.** `html2canvas` throws or renders blank on `oklch()` colors — the Tailwind v4 default palette. Either switch to `html-to-image` / `modern-screenshot` (`domToBlob`), or give `<OneSheet>` its own hex-only palette. Web fonts must be embedded or the capture falls back to a system face; simplest is to set `<OneSheet>` in a system stack and skip embedding.

**Style of the sheet** (conventions #68 and #70, both standing orders):

- Title is a **label**, never a claim: `2025 Arkansas Defensive Line — Room Table`. No verb of judgement.
- Bare worksheet look. No display headline, no dark bands, no rounded corners, no shadows, no colour-blocked headers.
- `@ArkansasQuant` between header and table. No URL.
- Footnote ≤2 sentences, source and cohort only. No editorial anywhere on the card.
- Filename `<team>-<tab>-<season>.png`.

Note: the portrait 4:5 / hero-number geometry is the format for a *post image*. This is a full table dump, so width-first at 1080 with natural height is correct. If you want the X-native crop as well, emit it as a second render, not as a constraint on this one.

---

## 4. Pinned norm row

**Definition (pick one, this is the ambiguity in the request).** Default shipped behaviour:

- Team-scoped table → **program norm**: that program's own 2025 value for the stat across the same position family.
- National position table → **position norm**: FBS median. P4 median as a toggle, where P4 = the four power conferences **plus Notre Dame** (`power_g5 = 'Power' OR team = 'Notre Dame'`, convention #46 — `power_g5='Power'` alone silently drops the Irish).

**Correctness constraint.** Convention #2: rate aggregates are snap- or attempt-weighted, never the mean of per-player rates, and sample size prints alongside. That means the norm row cannot be built by averaging the `v3_pv` column:

- Grades and already-snap-normalised rates → snap-weighted mean, fine.
- Compositional rates → recompute from summed numerator and denominator against the base PFF tables. Known denominators: `missed_tkl_rate = Σmissed / Σ(tackles + assists + missed)` (convention #10, stored 0–100); `stop_rate` uses the **run-defence-snap** denominator, not all defensive snaps (convention #69).
- Anything not correctly computable in v1 shows an **em-dash in the norm row**. Convention #5 — do not approximate it and do not quietly average the rates. Em-dash is informative; a wrong norm is not.

Print `n` (players / snaps) in the norm row's label cell.

**Safari gotcha.** `position: sticky` on `<tr>` is ignored in Safari. Apply it to the cells:

```css
thead th { position: sticky; top: 0; z-index: 3; background: #fff; }
tr.norm th, tr.norm td {
  position: sticky; top: var(--thead-h); z-index: 2;
  background: #fff; border-bottom: 2px solid #111; font-weight: 600;
}
```

Measure `--thead-h` from the rendered header; a hardcoded offset breaks when the header wraps at narrow widths.

The norm row is row 2 of the export as well, same rules, no colour fill.

---

## 5. Players → position sub-tabs

Eight tabs: **QB · Rushers · Receivers · OL · DL · LB · S · CB**.

### 5.1 Family mapping

`v3_pv.lfam` verified present for 2025: QB, HB, WR, TE, OT, OG, OC, ED, DI, LB, S, CB.

| Tab | lfam | Position column |
|---|---|---|
| QB | QB | dual / pocket (50+ `pff_rushing` attempts = dual, #36 V6) |
| Rushers | HB | HB |
| Receivers | WR, TE, HB | WR / TE / RB |
| OL | OT, OG, OC | **Spot**: LT / LG / C / RG / RT |
| DL | ED, DI | **Archetype**: ED / DT / NT |
| LB | LB | LB |
| S | S | S |
| CB | CB | CB |

**OL never collapses to "OL"** (convention #1). Spot = argmax of `pff_blocking.snap_counts_lt/lg/ce/rg/rt` on the max-snap 2025 row. Use `LATERAL (VALUES …) ORDER BY snaps DESC LIMIT 1` — `unnest` inside a subquery with `LIMIT` collapses to the first spot and silently mislabels the room (convention #25).

**DL archetype** (convention #25 thresholds, from 2025 usage on the max-snap row — never the listed position):

```
lfam = 'DI' AND align_agap_share >= 0.25  → NT
lfam = 'DI' AND align_agap_share <  0.25  → DT     (3-tech)
lfam = 'ED'                               → ED
```

`align_agap_share` is already carried in `v3_pv` for both ED and DI, so no base-table join is needed. Available if you want it later, not built now: ED splits JACK (`align_cov_share >= 0.10`) vs hand-down Edge.

### 5.2 Stat columns — verified `v3_pv` keys, HUDL excluded

HUDL stats are the `h_` prefix (convention #35). Ordered by the #36 validated screen priority, strongest first.

| Tab | Columns (left to right) |
|---|---|
| **QB** | `off_grade`, `qbp_clean_grade`, `qbp_pressure_epa_pos`, `ypa`, `qbp_blitz_epa_pos`, `comp_pct`, `qb_rating`, `qbp_blitz_ypa`, `qb_rush_ypa` |
| **Rushers** | `run_grade`, `ypa`, `yco_att`, `breakaway_pct`, `elusive`, `hb_pass_block_grade` |
| **Receivers** | route grade, YPRR, `caught_pct`, `drop_rate`, aDOT, `yac_per_rec`, `tgt_qbr`, `contested_rate`, alignment |
| **OL** | `pass_block_grade`, `off_grade`, `pressures_allowed_rate`, `pbe`, `run_block_grade`, `sacks_allowed_rate` |
| **DL** | `def_grade`, `prsh_grade`, `pressure_rate`, `pressures`, `sack_rate`, `stop_rate`, `run_def_grade`, `tfl_rate`, `tackles_total`, `missed_tkl_rate` |
| **LB** | `def_grade`, `run_def_grade`, `zone_cov_grade`, `cov_grade`, `align_cov_share`, `zone_stop_rate`, `qbr_against`, `stop_rate`, `missed_tkl_rate` |
| **S** | `def_grade`, `cov_grade`, `pbu_int_per_tgt`, `zone_cov_grade`, `zone_yds_per_cov_snap`, `align_cov_share`, `zone_mt_rate`, `stop_rate`, `run_def_grade` |
| **CB** | `man_catch_allowed`, `def_grade`, `align_cov_share`, `cov_grade`, `man_fi_rate`, `missed_tkl_rate`, `run_def_grade`, `qbr_against`, `pbu_int_per_tgt` |

Receivers takes its keys per source family (#36 V6 — order each row's own labeled screens, never index by family census position):

| Column | WR | TE | RB |
|---|---|---|---|
| Route grade | `route_grade` | `route_grade` | `hb_route_grade` |
| YPRR | `yprr` | `yprr` | `hb_yprr` |
| aDOT | `wr_adot` | `te_adot` | — |
| Alignment | `wr_slot_share` | `te_inline_rate` | `hb_recv_share` |

`caught_pct`, `drop_rate`, `yac_per_rec`, `tgt_qbr`, `contested_rate` are shared WR/TE keys; RB shows em-dash.

**Em-dash, never blank and never zero, where there is no sample.** Thin cells to expect: CB `man_*` (286 of 1,152 CBs have `man_cov_grade`), LB `man_*` (112), TE `contested_rate` (97).

### 5.3 Cost of the no-HUDL rule — flag before you ship

- **QB loses its strongest screen.** `h_qb_rush_epa` is HUDL and is the #1 validated QB predictor (r = .32 across archetypes; dual r = .303). `qb_rush_ypa` is PFF and sits in the table as a partial stand-in, but it is not the validated stat. The QB table is meaningfully weaker than the board.
- **Rushers loses** rush success, stuff%, target success, EPA/tgt and explosive-run screens — those live on the HUDL side, which is why the tab has six columns rather than nine.
- **ED and LB lose GetOff.**

None of this is a reason to reverse the rule; it is a reason not to read the tabs as equivalent to the wl27 board.

### 5.4 Default table state

- Season 2025, `snaps >= 100` (the population percentiles in #36 are cut at 100), filter control exposed.
- Sort by the tab's first column descending.
- Sample-size column (`snaps`) always visible — convention #2 requires n next to every rate.

### 5.5 SQL — new views, grants, policies

```sql
-- 1. player spine for the position tabs
create or replace view public.app_pos_players as
with base as (
  select v.player_id, v.season, v.lfam, nullif(v.team_name,'') as team_name,
         max(v.snaps) as snaps
  from public.v3_pv v
  where v.season = 2025 and v.stat not like 'h\_%'
  group by 1,2,3,4
),
agap as (
  select player_id, val as agap_share
  from public.v3_pv
  where season = 2025 and stat = 'align_agap_share'
)
select b.player_id, b.season, b.team_name, b.snaps, b.lfam,
       p.full_name, p.eligibility, p.height_inches, p.headshot_url,
       case b.lfam
         when 'QB' then 'QB'  when 'HB' then 'Rushers'
         when 'WR' then 'Receivers' when 'TE' then 'Receivers'
         when 'OT' then 'OL'  when 'OG' then 'OL' when 'OC' then 'OL'
         when 'ED' then 'DL'  when 'DI' then 'DL'
         else b.lfam end as tab,
       case
         when b.lfam = 'ED' then 'ED'
         when b.lfam = 'DI' and a.agap_share >= 0.25 then 'NT'
         when b.lfam = 'DI' then 'DT'
       end as archetype
from base b
left join agap a on a.player_id = b.player_id
left join public.players p on p.id = b.player_id;

-- 2. stat cells, HUDL excluded
create or replace view public.app_pos_stats as
select player_id, season, lfam, nullif(team_name,'') as team_name, stat, val
from public.v3_pv
where season = 2025 and stat not like 'h\_%';

-- 3. grants + policies, matching the existing authenticated_read_* pattern
grant select on public.app_pos_players, public.app_pos_stats to authenticated;
```

Gates before merging:

1. `nullif(team_name,'')` on every team join — HUDL rows carry empty-string team, not NULL (convention #35). Present above for safety even though `h_` rows are filtered.
2. Views default to `security_invoker = off`, so they read `v3_pv` with the owner's rights and bypass its missing grant. That is deliberate here. Supabase's linter will flag it as a SECURITY DEFINER view — expected, not a defect, but decide it consciously rather than by accident.
3. **Assert from a real browser session, not the SQL editor**, that `app_pos_players` returns rows before wiring the UI. The SQL editor runs as owner and will pass even when the app 404s. This is exactly how the Coordinator tab shipped broken.
4. Count check per convention #50: 2025 FBS = 136 teams. If a team join returns fewer, the join is broken — do not rank against a reduced denominator.

---

## 6. Claude Code prompt

```
Work in AustinAlexander01/cfb-quant. Branch: feat/mobile-density-onesheeter-position-tabs.
Read cfb-quant-mobile-onesheeter-spec.md (attached) in full first — it carries verified
Supabase column names and four RLS/grant gates. Do not invent column names; every stat key
in section 5.2 was verified against v3_pv for season 2025 on 2026-08-14.

Do these four, in this order, each as its own commit:

1. TEAMS SEARCH (spec §1). This is a vertical-space problem, not an ordering problem — only
   one result fits above the keyboard. In order: (a) collapse the page title and the
   email/SIGN OUT row to zero height while the search input is focused, leaving a compact
   sticky nav, restore on blur; (b) conference section headers become an 18px inline band,
   11px uppercase, no vertical margin; (c) team cards become 34px rows — 20px logo, 15px name,
   hairline divider, no card background or radius; (d) two-column grid while a query is active
   above 360px viewport width, single column below and in the empty-query browse state.
   Size the scroll container off window.visualViewport so it sits above the iOS keyboard.
   Acceptance: typing "Fl" on an iPhone 15 viewport shows all 7 matches without scrolling.

2. COACH COLUMNS (spec §2). table-layout: fixed; below 480px render HC/OC/DC as first-initial
   + surname with the full name in title; 11ch max-width with ellipsis; 13px, 4px 6px padding,
   tabular-nums on numerics. Do not apply any of this inside the export render.

3. ONE SHEETER (spec §3). This is the one with real iOS traps — read §3 (a) through (d) before
   writing code. New <OneSheet> component mounted offscreen at fixed 1080px width containing
   header, @ArkansasQuant watermark, norm row, all table rows, footnote, and nothing else.
   Capture with html-to-image or modern-screenshot, NOT html2canvas (oklch). Compute pixelRatio
   from the 16M-pixel iOS canvas cap. Deliver via navigator.share({files}) with an anchor
   fallback on desktop and a long-press <img> modal fallback. Pre-render the blob on sheet open
   so the share call sits inside the user gesture. Style per §3: label-style title, bare
   worksheet look, no editorial, footnote max 2 sentences, no URL in the watermark.
   Then add the pinned norm row (spec §4) — sticky on the cells, not the <tr>, or Safari
   ignores it. Program norm for team tables, FBS median for national tables, em-dash for any
   stat whose norm cannot be computed with the correct denominator. Print n.

4. PLAYERS POSITION TABS (spec §5). Eight tabs: QB, Rushers, Receivers, OL, DL, LB, S, CB.
   Add the two views + grants from §5.5 as a migration; do not query v3_pv from the browser,
   it has no authenticated grant and will 404. OL shows LT/LG/C/RG/RT, never "OL". DL shows an
   ED/DT/NT archetype column from align_agap_share >= 0.25. Stat columns exactly as listed in
   §5.2, HUDL (h_ prefix) excluded, em-dash where there is no sample. Default snaps >= 100,
   snaps column always visible.

Before opening the PR: run the four gates in §5.5, and confirm the new views return rows from
an authenticated browser session rather than the SQL editor.
```

---

## 7. Running this yourself, without a local clone

Claude Code on the web runs in an Anthropic-hosted VM, not on your machine — no install, no admin rights, no clone. You drive it from claude.ai/code in a browser or the **Code tab in the Claude mobile app**. It clones the repo, works, and pushes a branch you review. Sessions persist across devices, so a task started on the laptop is reviewable from the phone.

Research preview for Pro, Max and Team plans, and Enterprise with premium or Chat + Claude Code seats.
Docs: https://code.claude.com/docs/en/web-quickstart

**Prerequisite — the only thing Austin has to do.** A cloud session can use any repository your connected GitHub account can see. The repo is private and owned by Austin, so he adds you as a collaborator: github.com/AustinAlexander01/cfb-quant → Settings → Collaborators → Add people. That is the whole handoff. Installing the Claude GitHub App on the repo is only needed for auto-fix on PRs, not for this.

**One-time setup.**

1. claude.ai/code → sign in → follow the prompt to connect GitHub.
2. Onboarding creates a cloud environment named **Default** with Trusted network access. Keep the defaults; this repo needs nothing else.
3. If the page shows only a GitHub login button, the account isn't connected yet. If you get "Not available for the selected organization," an Enterprise Owner has to enable it — likely relevant if you're signed in on a Walmart-managed account rather than a personal one.

**Per task.**

1. Pick `AustinAlexander01/cfb-quant` in the repository selector and set the branch.
2. Set the mode dropdown to **Plan** for the first pass so it proposes an approach before editing. **Accept edits** once you trust the shape of the change.
3. Paste the prompt from §6. Submit.
4. Claude pushes a branch. Vercel builds a preview deployment for it — open that URL on the iPhone. All four items here are only judgeable on a phone, so this preview is the actual test, not the diff.
5. Leave inline comments on diff lines; they bundle with your next message so you don't have to describe where the problem is.
6. **Create PR** from the diff view when it looks right. The session stays live after the PR — paste CI output or review comments straight in.

**Two things that make this workable from a phone.**

- Have Austin commit this spec to the repo as `docs/cfb-quant-mobile-onesheeter-spec.md`. Then your prompt is one line you can actually type on a phone: *"Read docs/cfb-quant-mobile-onesheeter-spec.md and implement §1 only. Push a branch."* Ship one section per session rather than all four at once.
- Sessions can be pre-filled by URL, so a bookmark saves the repo selection every time:
  `https://claude.ai/code?repositories=AustinAlexander01/cfb-quant&prompt=Read%20docs/cfb-quant-mobile-onesheeter-spec.md%20and%20implement%20%C2%A71%20only.`

Do the four items as four sessions on four branches, not one. Item 3 is the one most likely to need several rounds, and you don't want the search fix stuck behind it.

---

## 8. Open item

"Program norm" is specced above as the program's own 2025 value for that stat and position family. If it was meant as the FBS/P4 positional benchmark instead, only §4's default flips — the sticky mechanics, the export inclusion and the denominator rules are unchanged.
