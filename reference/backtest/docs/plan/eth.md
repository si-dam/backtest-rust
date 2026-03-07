<proposed_plan>
# ETH Compressed Display Mode With Clickable Session Breaks

## Summary
Add a new optional chart display mode that visually hides ETH candles while preserving full-session bars in memory for calculations. In this mode, the time axis is compressed (ETH bars removed from displayed series), a clickable break marker appears per session day, and clicking a marker toggles ETH visibility for that day only. Defaults and decisions are locked as follows: mode off by default, fixed RTH window `09:30-16:00 ET`, one break per session day, hidden-session overlays hidden with ETH, and expanded-day state persisted by context.

## Scope and Behavior
1. Add a new display mode toggle in Settings.
2. Keep full bars loaded from `/chart/bars` unchanged for calculations and future indicators.
3. Build a derived display dataset from full bars:
4. If mode off: display all bars (current behavior).
5. If mode on and a session day is collapsed: display only that day’s RTH bars.
6. If mode on and a session day is expanded: display full ETH+RTH bars for that day.
7. Render one clickable break marker for each collapsed session day that has hidden ETH bars.
8. Clicking a marker toggles that session day expanded/collapsed.
9. Overlays in hidden ETH intervals are not rendered until that day is expanded.
10. No backend API changes.

## Implementation Details

### Data model additions in chart UI
1. In `app/api/routes_chart_ui.py`, add state variables:
2. `latestBarsFull` for all fetched bars.
3. `latestBarsDisplay` for currently rendered bars.
4. `ethCompressionEnabled` boolean.
5. `expandedEthSessionDays` as `Set<string>` where value is session day `YYYY-MM-DD`.
6. `visibleBarTimesSet` as `Set<number>` for quick visibility checks.
7. `sessionBreakMarkers` array with marker metadata: `sessionDay`, `anchorTs`, `hiddenStartTs`, `hiddenEndTs`, `hiddenCount`.

### Session classification and grouping
1. Add helper to classify each bar using fixed ET window:
2. Convert timestamp to ET.
3. RTH check: `09:30 <= time < 16:00`.
4. Session day key logic: bars before `09:30` belong to previous session day; bars from `09:30` onward belong to same calendar day.
5. Group full bars by session day, split each group into RTH and ETH arrays, keep original chronological order.

### Display projection pipeline
1. Add a projector function that returns:
2. `displayBars` for candle/volume series.
3. `breakMarkers` for collapsed days.
4. `visibleBarTimesSet`.
5. Projection rules:
6. Mode off: `displayBars = latestBarsFull`, `breakMarkers = []`.
7. Mode on collapsed day: include only RTH bars, create one break marker if ETH exists.
8. Mode on expanded day: include all bars for that session day.
9. Break marker anchor: first visible RTH bar timestamp for that day; fallback to nearest visible day boundary if needed.

### Rendering layers and interactions
1. Add a dedicated HTML layer above chart canvas for break markers, for example `#tv-session-break-layer`.
2. Render one marker per `sessionBreakMarkers` entry:
3. Visually obvious “break” chip/handle and dashed separator style.
4. Click target with `data-session-day`.
5. Clicking marker toggles `expandedEthSessionDays`, rebuilds display projection, reapplies series data, rerenders overlays/drawings/markers.
6. Re-render break markers on:
7. chart load/update,
8. resize,
9. visible logical range change,
10. mode toggle,
11. timezone/symbol/timeframe/range context changes.

### Series updates and existing feature integration
1. Replace direct `bars -> candleSeries.setData` path with projected display bars.
2. Volume series uses projected display bars.
3. Keep `latestBarsFull` as canonical source for calculations/future indicators.
4. Large orders:
5. Keep backend query unchanged.
6. Anchor/render against display times only so hidden ETH dots do not show.
7. Backtest markers:
8. Filter markers to visible display timestamps in compressed mode.
9. Drawings:
10. Prevent hidden-time snap artifacts by adjusting `chartTimeToX` behavior in compressed mode so non-visible times return `null` instead of snapping to previous visible bar.
11. Volume profiles/area profiles:
12. Skip rendering profile elements that map entirely to hidden ETH intervals while collapsed.
13. Show them automatically when day is expanded.

### Settings and persistence
1. Add settings controls:
2. `ETH Display Mode` toggle in Settings (default off).
3. Optional helper action: `Collapse All ETH Days` when mode is on.
4. Local storage:
5. `chart-ui.eth-compression.v1` for mode enabled state.
6. `chart-ui.eth-expanded.v1::<symbol>::<timeframe>::<timezone>::<start>::<end>` for expanded session days.
7. On context load:
8. Restore mode.
9. Restore expanded days from context key.
10. Prune expanded days not present in loaded data.

### Guardrails
1. Intraday-only behavior:
2. For `1d` timeframe, force mode off/disabled in UI.
3. Keep current behavior unchanged when mode is off.
4. Preserve auto-load flow and status messages.

## Public Interfaces / Contract Changes
1. Chart UI DOM additions in `app/api/routes_chart_ui.py`:
2. New settings input ID for ETH compression toggle.
3. New break marker layer element ID.
4. New localStorage keys:
5. `chart-ui.eth-compression.v1`
6. `chart-ui.eth-expanded.v1::<context>`
7. No changes to backend HTTP endpoints or payload schemas.

## Test Plan

### Unit/contract tests
1. Extend `tests/unit/test_chart_ui.py` to assert presence of:
2. ETH compression setting control ID.
3. Session break layer ID.
4. New storage key strings.
5. New JS function names for projection and break rendering/toggling.

### Behavioral scenarios to verify manually
1. Mode off: chart identical to current behavior.
2. Mode on: ETH candles hidden by default, compressed axis visible, break marker shown per session day with hidden ETH.
3. Click break marker: only that day expands ETH; second click re-collapses.
4. Multiple days: independent per-day expansion.
5. Overlays: hidden ETH overlays not visible until expanded.
6. Timeframe change to `1d`: mode disabled/inactive.
7. Reload same context: expanded days restored.
8. Change symbol/timeframe/range/timezone: context-scoped expansion state behaves as defined.
9. Performance check on wide ranges: projection and rerender stay responsive.

## Assumptions and Defaults
1. RTH boundary source is fixed `09:30-16:00 ET`.
2. One clickable ETH break per session day.
3. ETH compressed mode default is off.
4. Expanded ETH persistence is context-scoped by symbol+timeframe+timezone+start+end.
5. Hidden ETH bars remain in memory (`latestBarsFull`) for calculations and future indicators.
6. No backend changes are required for v1.
</proposed_plan>
