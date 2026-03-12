import type { CanvasRenderingTarget2D } from "fancy-canvas";
import type {
  IPrimitivePaneRenderer,
  IPrimitivePaneView,
  ISeriesPrimitive,
  PrimitivePaneViewZOrder,
  SeriesAttachedParameter,
  Time,
  UTCTimestamp,
} from "lightweight-charts";
import type { Profile } from "../lib/api";

interface RenderLevel {
  y: number;
  value: number;
  normalized: number;
}

interface RenderSegment {
  left: number;
  right: number;
  levels: RenderLevel[];
  pocY: number | null;
  valueAreaLowY: number | null;
  valueAreaHighY: number | null;
}

function toUtcTimestamp(value: string): UTCTimestamp {
  return Math.floor(new Date(value).getTime() / 1000) as UTCTimestamp;
}

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
}

function sortNumbers(a: number, b: number) {
  return a - b;
}

function buildLevelHeight(levels: RenderLevel[], index: number) {
  const previous = levels[index - 1];
  const current = levels[index];
  const next = levels[index + 1];

  if (!previous && !next) {
    return 6;
  }

  const gaps = [
    previous ? Math.abs(current.y - previous.y) : Number.POSITIVE_INFINITY,
    next ? Math.abs(next.y - current.y) : Number.POSITIVE_INFINITY,
  ].filter(Number.isFinite);

  if (!gaps.length) {
    return 6;
  }

  return clamp(Math.min(...gaps) * 0.7, 2, 14);
}

class PresetProfilesRenderer implements IPrimitivePaneRenderer {
  constructor(
    private readonly segments: RenderSegment[],
    private readonly showValueArea: boolean,
  ) {}

  draw(target: CanvasRenderingTarget2D): void {
    target.useMediaCoordinateSpace(({ context }) => {
      context.save();
      context.lineCap = "round";

      for (const segment of this.segments) {
        const segmentWidth = segment.right - segment.left;
        if (segmentWidth < 8 || !segment.levels.length) {
          continue;
        }

        const profileWidth = clamp(segmentWidth * 0.38, 28, 140);
        const histogramRight = segment.right - 6;
        const histogramLeft = Math.max(segment.left + 6, histogramRight - profileWidth);
        const drawableWidth = histogramRight - histogramLeft;
        if (drawableWidth <= 0) {
          continue;
        }

        if (
          this.showValueArea &&
          segment.valueAreaHighY !== null &&
          segment.valueAreaLowY !== null
        ) {
          const bandTop = Math.min(segment.valueAreaHighY, segment.valueAreaLowY);
          const bandBottom = Math.max(segment.valueAreaHighY, segment.valueAreaLowY);
          context.fillStyle = "rgba(250, 204, 21, 0.08)";
          context.fillRect(segment.left + 1, bandTop, segmentWidth - 2, bandBottom - bandTop);

          context.strokeStyle = "rgba(250, 204, 21, 0.28)";
          context.lineWidth = 1;
          context.beginPath();
          context.moveTo(segment.left + 1, segment.valueAreaHighY);
          context.lineTo(segment.right - 1, segment.valueAreaHighY);
          context.moveTo(segment.left + 1, segment.valueAreaLowY);
          context.lineTo(segment.right - 1, segment.valueAreaLowY);
          context.stroke();
        }

        context.fillStyle = "rgba(96, 165, 250, 0.22)";
        for (let index = 0; index < segment.levels.length; index += 1) {
          const level = segment.levels[index];
          const height = buildLevelHeight(segment.levels, index);
          const width = Math.max(1, level.normalized * drawableWidth);
          const x = histogramRight - width;
          const y = level.y - height / 2;
          context.fillRect(x, y, width, height);
        }

        if (segment.pocY !== null) {
          context.strokeStyle = "rgba(248, 113, 113, 0.9)";
          context.lineWidth = 2;
          context.beginPath();
          context.moveTo(segment.left + 1, segment.pocY);
          context.lineTo(segment.right - 1, segment.pocY);
          context.stroke();
        }
      }

      context.restore();
    });
  }
}

class PresetProfilesPaneView implements IPrimitivePaneView {
  constructor(private readonly source: PresetProfilesPrimitive) {}

  zOrder(): PrimitivePaneViewZOrder {
    return "bottom";
  }

  renderer(): IPrimitivePaneRenderer | null {
    return this.source.renderer();
  }
}

export class PresetProfilesPrimitive implements ISeriesPrimitive<Time> {
  private readonly paneView = new PresetProfilesPaneView(this);
  private readonly paneViewsList = [this.paneView];
  private attachedParams: SeriesAttachedParameter<Time> | null = null;
  private profiles: Profile[] = [];
  private showValueArea = true;

  attached(param: SeriesAttachedParameter<Time>): void {
    this.attachedParams = param;
  }

  detached(): void {
    this.attachedParams = null;
  }

  paneViews(): readonly IPrimitivePaneView[] {
    return this.paneViewsList;
  }

  setData(profiles: Profile[], showValueArea: boolean): void {
    this.profiles = profiles;
    this.showValueArea = showValueArea;
    this.attachedParams?.requestUpdate();
  }

  renderer(): IPrimitivePaneRenderer | null {
    const segments = this.renderSegments();
    if (!segments.length) {
      return null;
    }

    return new PresetProfilesRenderer(segments, this.showValueArea);
  }

  private renderSegments(): RenderSegment[] {
    if (!this.attachedParams || !this.profiles.length) {
      return [];
    }

    const { chart, series } = this.attachedParams;
    const timeScale = chart.timeScale();
    const segments: RenderSegment[] = [];

    for (const profile of this.profiles) {
      const startX = timeScale.timeToCoordinate(toUtcTimestamp(profile.start));
      const endX = timeScale.timeToCoordinate(toUtcTimestamp(profile.end));
      if (startX === null || endX === null) {
        continue;
      }

      const left = Math.min(startX, endX);
      const right = Math.max(startX, endX);
      const maxLevelValue = Math.max(...profile.levels.map((level) => level.value), 0);
      if (right - left < 8 || maxLevelValue <= 0) {
        continue;
      }

      const levels = profile.levels
        .map((level) => {
          const y = series.priceToCoordinate(level.price_level);
          if (y === null) {
            return null;
          }

          return {
            y: Number(y),
            value: level.value,
            normalized: level.value / maxLevelValue,
          };
        })
        .filter((level): level is RenderLevel => level !== null)
        .sort((a, b) => sortNumbers(a.y, b.y));

      if (!levels.length) {
        continue;
      }

      const pocY =
        profile.value_area_poc !== null ? series.priceToCoordinate(profile.value_area_poc) : null;
      const valueAreaLowY =
        profile.value_area_low !== null ? series.priceToCoordinate(profile.value_area_low) : null;
      const valueAreaHighY =
        profile.value_area_high !== null ? series.priceToCoordinate(profile.value_area_high) : null;

      segments.push({
        left,
        right,
        levels,
        pocY,
        valueAreaLowY,
        valueAreaHighY,
      });
    }

    return segments;
  }
}
