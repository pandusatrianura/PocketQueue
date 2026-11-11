---
name: upwork-proposal-writer
description: Draft or review an Upwork proposal (cover letter) using the five-move method extracted from a Top Rated Plus freelancer's winning proposals, corrected for the AI era. Use when the user shares an Upwork job post and wants a proposal drafted, or shares an existing proposal draft and wants it reviewed. Triggers include "write a proposal", "apply for this job", "bid on this", "cover letter for this Upwork job", "review my proposal". Works for design, dev, and adjacent creative/tech freelance jobs.
---

# Upwork Proposal Writer

Draft proposals the way a Top Rated Plus freelancer actually writes them, not the way template blogs say to. The method below was extracted from real winning proposals (design/dev niche, top 3% Upwork tier) and cross-checked against Upwork's own published guidance. Where the two disagree, this file says so.

## Inputs you need before drafting

Ask for anything missing. Never invent these.

1. **The job post**, pasted in full.
2. **The user's identity line facts:** role, years of experience, niche specialization.
3. **3-5 portfolio links or samples** that match THIS job's niche. Matched proof beats impressive-but-unrelated proof. If the user gives you ten links, pick the closest 3-5 and say why.
4. **One relevant outcome** they can claim (a shipped result, a metric, a client situation similar to this job). The outcome belongs in the credibility move. If the user can't supply one, draft without it and say so. Never invent one.
5. Optional: rate intentions, availability, anything the client asked applicants to answer.

Profile shortcut: if a stored profile file exists (`upwork-profile.md` in the working directory or `~/.claude/`), read identity, niche, links, and rate from it and ask only for what's missing or not stated concretely (a rate line that says "ask me" means ask). Never ask for what the file already answers.

Before drafting, check the job's core ask against the profile. If a major requirement sits outside what the profile actually covers, flag the stretch in one line before the draft. The draft itself only claims what's real.

## Mode 1: Draft (primary)

### The five moves, in order

**Move 1: credibility line.** One sentence: identity + years + niche. Example shape: "I'm a UI/UX designer with 5+ years specializing in SaaS dashboards." This is the first thing the client sees in the results-list preview, alongside the freelancer's photo, badges, rate, and Job Success Score. It has to establish fit in one line.

**Move 2: curated proof.** Immediately place the 3-5 matched portfolio links or attachments. Before any pitch. Upwork's own data: freelancers with published portfolios are hired nine times more often. The curation is the skill: e-commerce links for an e-commerce job, motion links when the brief mentions motion. If the stored profile only has generic homepage links, still ask for samples matched to this job's niche. Matched proof beats stored convenience.

**Move 3: mirror the brief.** Restate the client's requirements back as a short bullet list ("Based on your brief, you need someone who can combine: ..."). This proves close reading, which is the one thing a template blast can never fake. For short invite replies, compress this to a single sentence naming their specific situation.

**Move 4: phased plan (scoped and fixed-price jobs only).** A numbered 2-4 step delivery plan ("1. staging prototype, 2. full storefront, 3. portal, 4. integration + testing"). It de-risks the hire by showing the shape of the work. Skip this entirely on quick invite replies; adding it there reads as boilerplate.

**Move 5: question close.** End with either 2-3 specific clarifying questions ("Quick questions so I don't guess wrong: ...") or one low-pressure open question ("What does success look like for this?"). A question invites a reply. Never hard-push a call; if the client proposes a call, accept immediately with zero sales copy.

**High-stakes escalation (optional, big scoped jobs only).** When the job is large enough to justify an hour of unpaid work, upgrade Move 2 from links to a small custom artifact: a two-minute walkthrough video, a quick audit of their current site, or one annotated mock. Practitioners running high-ticket funnels report this as their highest-converting move. Never do it for routine jobs; the economics only work when the contract size does.

**Remember what the proposal is for.** Its only job is to earn a profile visit and a reply. The profile closes what the proposal opens, so the portfolio there must be complete and current before any proposal volume makes sense (that is where the 9x hire-rate effect lives).

### Length rules (hard)

- 200-300 words maximum for standard proposals. Three short paragraphs is the sweet spot.
- 3-4 sentences total for replies to client invitations.
- Length scales with job complexity, never with enthusiasm.

### Voice rules (hard)

- Write like a person. Contractions, short sentences, plain words.
- No em dashes anywhere.
- Zero filler openers: no "I'm excited," no "I hope this finds you well," no "I came across your posting."
- Never include contact information in the cover letter (Upwork policy; it can get the proposal flagged).
- Never paste rates, client names, or outcomes from OTHER clients' private work into a proposal.

## Mode 2: Review

Score the user's draft against each move (present/weak/missing), then rewrite the weakest two moves rather than listing adjectives. Check, in order: does line 1 establish fit; is proof present, early, and matched to the niche; is the brief mirrored or is this a template; is there a plan when the job is scoped; does it end with a question; is it under 300 words; do any banned phrases appear.

## The AI-era layer (why this method still wins)

Upwork now ships Uma, its own in-platform AI that drafts cover letters for any freelancer, free tier included. The floor has moved: a grammatically clean, vaguely personalized proposal is now the baseline, mass-produced at zero effort. What Uma cannot generate is the content of Moves 2 and 3: proof that exists and matches, and reading comprehension specific enough to mirror. That is where this method concentrates its effort.

Two corrections to the official guidance, from field results:

1. **Official guidance says open by addressing the client's needs. The winning pattern opens with a one-line credibility statement, then mirrors needs in Move 3.** Both beat generic openers; the invariant is that the first two visible lines must establish specific fit, because that preview decides whether the proposal gets opened at all.
2. **Official guidance treats AI as the drafting helper. The field correction: use AI for speed, and say so when relevant.** A real winning proposal stated AI-assisted iteration speed as an explicit differentiator. Stating the capability plainly beats hiding it, and both beat outsourcing the thinking to it.

## Sources and provenance

Method extracted from a Top Rated Plus freelancer's winning proposal threads (2025-2026, design/dev niche). Official guidance distilled from Upwork's published resources, recommended reading:

- How To Create a Proposal on Upwork That Wins Jobs: https://www.upwork.com/resources/how-to-create-a-proposal-that-wins-jobs
- Upwork Cover Letter Tips: https://www.upwork.com/resources/cover-letter-tips
- How To Reply to an Upwork Job Invitation: https://www.upwork.com/resources/job-invitation-reply
- Why Are My Proposals Not Being Viewed: https://www.upwork.com/resources/why-are-my-proposals-not-being-viewed
- How To Differentiate Yourself as a Freelancer: https://www.upwork.com/resources/how-to-differentiate-yourself-as-a-freelancer
- 15 Tips To Make Your Freelancer Profile Stand Out: https://www.upwork.com/resources/freelancer-profile-tips
- Third-party deep dives: Evan Fisher's Upwork cover letter videos (YouTube); Nick Saraev on custom-artifact proposals.

This skill is not affiliated with or endorsed by Upwork.
