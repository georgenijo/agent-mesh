---
name: Agent Mesh
colors:
  surface: '#0b1326'
  surface-dim: '#0b1326'
  surface-bright: '#31394d'
  surface-container-lowest: '#060e20'
  surface-container-low: '#131b2e'
  surface-container: '#171f33'
  surface-container-high: '#222a3d'
  surface-container-highest: '#2d3449'
  on-surface: '#dae2fd'
  on-surface-variant: '#bbc9cd'
  inverse-surface: '#dae2fd'
  inverse-on-surface: '#283044'
  outline: '#859397'
  outline-variant: '#3c494c'
  surface-tint: '#2fd9f4'
  primary: '#8aebff'
  on-primary: '#00363e'
  primary-container: '#22d3ee'
  on-primary-container: '#005763'
  inverse-primary: '#006877'
  secondary: '#ffb95f'
  on-secondary: '#472a00'
  secondary-container: '#ee9800'
  on-secondary-container: '#5b3800'
  tertiary: '#68f5b8'
  on-tertiary: '#003824'
  tertiary-container: '#46d89d'
  on-tertiary-container: '#005a3d'
  error: '#ffb4ab'
  on-error: '#690005'
  error-container: '#93000a'
  on-error-container: '#ffdad6'
  primary-fixed: '#a2eeff'
  primary-fixed-dim: '#2fd9f4'
  on-primary-fixed: '#001f25'
  on-primary-fixed-variant: '#004e5a'
  secondary-fixed: '#ffddb8'
  secondary-fixed-dim: '#ffb95f'
  on-secondary-fixed: '#2a1700'
  on-secondary-fixed-variant: '#653e00'
  tertiary-fixed: '#6ffbbe'
  tertiary-fixed-dim: '#4edea3'
  on-tertiary-fixed: '#002113'
  on-tertiary-fixed-variant: '#005236'
  background: '#0b1326'
  on-background: '#dae2fd'
  surface-variant: '#2d3449'
typography:
  display-mono:
    fontFamily: JetBrains Mono
    fontSize: 24px
    fontWeight: '700'
    lineHeight: 32px
    letterSpacing: -0.02em
  headline-sm:
    fontFamily: Inter
    fontSize: 18px
    fontWeight: '600'
    lineHeight: 24px
  body-md:
    fontFamily: Inter
    fontSize: 14px
    fontWeight: '400'
    lineHeight: 20px
  code-sm:
    fontFamily: JetBrains Mono
    fontSize: 12px
    fontWeight: '400'
    lineHeight: 18px
  label-xs:
    fontFamily: JetBrains Mono
    fontSize: 10px
    fontWeight: '600'
    lineHeight: 12px
    letterSpacing: 0.05em
rounded:
  sm: 0.125rem
  DEFAULT: 0.25rem
  md: 0.375rem
  lg: 0.5rem
  xl: 0.75rem
  full: 9999px
spacing:
  unit: 4px
  gutter: 12px
  margin-safe: 16px
  panel-padding: 8px
---

## Brand & Style
The design system is engineered for **Agent Mesh**, a local-first coordination layer for autonomous AI coding agents. The brand personality is technical, precise, and utilitarian, evoking the feeling of a "shared nervous system" for machine intelligence. 

The aesthetic is a hybrid of **Minimalism** and **Technical Brutalism**. It prioritizes high-density information display and real-time state visualization over decorative elements. Every visual choice is functional, designed to minimize latency perception and maximize the operator's situational awareness of the agentic swarm. 

**Core Principles:**
- **Local-First Speed:** Instantaneous feedback through local state optimistic UI.
- **Agent Visibility:** Clear visual differentiation between "Idle," "Active," and "Locked" states.
- **Shared Blackboard:** A focus on the centralized context that all agents read from and write to.

## Colors
The palette is centered on a "Deep Space" dark mode to reduce eye strain during long-form debugging and monitoring. 

- **Primary (Nervous System Cyan):** Used for active data flow, agent pulse, and primary actions. It represents the "synapse" of the system.
- **Secondary (Conflict Amber):** Reserved for resource locks, race conditions, and pending approvals.
- **Tertiary (Success Emerald):** Indicates committed merges, successful syncs, and validated agent tasks.
- **Neutral (Slate & Carbon):** Structural colors that define the hierarchy of the dashboard and terminal panels.

## Typography
The system utilizes a dual-font strategy to distinguish between UI controls and the underlying data layer.

- **Inter:** Used for all structural navigation, settings, and high-level descriptions. It provides a clean, neutral interface that stays out of the way.
- **JetBrains Mono:** The primary font for the "Blackboard" (the shared state), logs, agent IDs, and code blocks. Monospaced characters ensure that data columns and diffs align perfectly across the mesh.

Scale is kept small to facilitate high-density information environments. All labels should be uppercase when using JetBrains Mono to reinforce the "terminal" aesthetic.

## Layout & Spacing
This design system employs a **Fluid Panel Grid**. Rather than a traditional column-based website layout, it uses a modular tile system that scales based on the viewport.

- **High Density:** Padding is aggressive. Standard component height is 28px or 32px to fit more telemetry on screen.
- **Panels:** The UI is divided into functional zones: Sidebar (Agents), Center (Shared Blackboard), and Bottom (Log Stream).
- **Rhythm:** A 4px baseline grid governs all spacing. Gutters are kept tight (12px) to maintain the feeling of a unified machine interface.

## Elevation & Depth
Depth is communicated through **Tonal Layering** and **Subtle Outlines** rather than traditional shadows.

- **Base Layer:** `#020617` (The Canvas).
- **Surface Layer:** `#0F172A` (Panels and Modules).
- **Active Layer:** `#1E293B` (Hover states and active tooltips).
- **Borders:** All panels use a 1px solid border (`#1E293B`). There are no ambient shadows. 
- **Backdrop Blur:** Use a heavy blur (12px) for modal overlays to maintain context of the background "mesh" while focusing on a specific agent's detail.

## Shapes
The shape language is "Soft-Technical." UI elements use a subtle 4px (`0.25rem`) radius to prevent the interface from feeling overly hostile or "retro," while maintaining a precise, modern edge.

- **Buttons/Inputs:** 4px radius.
- **Status Indicators:** Perfect circles (for pulsing presence dots).
- **Terminal Panels:** 0px radius at the edges of the viewport, 4px when floating.

## Components

### Buttons & Inputs
Buttons are low-profile. The primary action uses a subtle Cyan glow on hover. Inputs are "Ghost Style" with a `1px` border that brightens when focused. No background fill for inputs on surface layers.

### Presence Dots (Nervous System Pulse)
Active agents are represented by a pulsing dot indicator. 
- **Idle:** Solid Slate.
- **Thinking:** Cyan pulse (Scale 1.0 to 1.2, 2s duration).
- **Locked:** Amber pulse (Fast, 0.5s duration).

### Terminal Log Entries
Logs must use JetBrains Mono. Each entry should be prefixed with a timestamp and a color-coded "origin agent" tag. Use high-contrast color coding for log levels: `DEBUG` (Slate), `INFO` (Cyan), `WARN` (Amber), `ERROR` (Red).

### Claim Badges
Small, high-contrast badges used when an agent "claims" a file or resource. These should use the `label-xs` typography and have a background color corresponding to the agent's unique ID tint.

### Data Grids
Bordered rows with no horizontal lines—only vertical separators between key columns. Row hovering should highlight the entire line in a translucent Cyan (`#22D3EE` at 5% opacity).