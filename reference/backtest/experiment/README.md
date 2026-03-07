# Volume Profile Memory Experiment

This experiment builds all volume profile presets (`week`, `day`, `rth`, `eth`) with `tick_aggregation=1` over the last 7 days of available ticks, then reports memory usage.

## Run

```bash
python experiment/volume_profile_memory.py
```

Optional flags:

```bash
python experiment/volume_profile_memory.py --symbol NQH26 --profile-timezone America/New_York
```

Output report:

- `experiment/volume_profile_memory_report.json`
