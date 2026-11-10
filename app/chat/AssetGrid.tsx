"use client";

import { useMemo, useRef, useSyncExternalStore } from "react";
import { AgGridReact, type CustomCellRendererProps } from "ag-grid-react";
import { AllCommunityModule, ModuleRegistry, themeQuartz, type ColDef } from "ag-grid-community";
import type { ComparisonRow } from "./types";
import styles from "./chat.module.css";

ModuleRegistry.registerModules([AllCommunityModule]);

const theme = themeQuartz.withParams({
  accentColor: "#0f6fb4",
  foregroundColor: "#08213d",
  headerTextColor: "#4a6a8c",
  headerFontWeight: 700,
  borderColor: "#dbe6f1",
  rowHoverColor: "#f2f7fc",
  oddRowBackgroundColor: "#fbfdff",
  fontFamily: "inherit",
  fontSize: 13,
  spacing: 6,
  wrapperBorderRadius: 10,
});

const NARROW = "(max-width: 640px)";
const watchNarrow = (onChange: () => void) => {
  const query = window.matchMedia(NARROW);
  query.addEventListener("change", onChange);
  return () => query.removeEventListener("change", onChange);
};

/** CSV headers carry their units, since the file has no bars or tooltips. */
const CSV_UNITS: Partial<Record<keyof ComparisonRow, string>> = {
  overallScore: " (0-100)", priceRisk: " (0-100, higher is riskier)", marketStanding: " (0-100)", liquidity: " (0-100)",
  volatilityPct: " (%)", maxDrawdownPct: " (%)", belowHighPct: " (%)",
};

/** Not measured always sorts last, whichever way the column is sorted. */
function nullsLast(left: number | null, right: number | null, _a: unknown, _b: unknown, descending: boolean): number {
  if (left === null && right === null) return 0;
  if (left === null) return descending ? -1 : 1;
  if (right === null) return descending ? 1 : -1;
  return left - right;
}

function ScoreCell({ value, higherIsBetter }: CustomCellRendererProps<ComparisonRow, number | null> & { higherIsBetter: boolean }) {
  if (value === null || value === undefined) return <span className={styles.gridEmpty}>Not measured</span>;
  const good = higherIsBetter ? value : 100 - value;
  const tone = good >= 60 ? styles.gridGood : good >= 40 ? styles.gridFair : styles.gridWeak;
  return (
    <span className={styles.gridScore}>
      <strong>{value}</strong>
      <span className={styles.gridBar} aria-hidden><span className={tone} style={{ width: `${Math.max(2, Math.min(100, value))}%` }} /></span>
    </span>
  );
}

function AssetCell({ data, compact }: CustomCellRendererProps<ComparisonRow> & { compact: boolean }) {
  if (!data) return null;
  return (
    <span className={styles.gridAsset} title={compact ? data.name : undefined}>
      {compact ? <strong>{data.asset}</strong> : <strong>{data.name} <span>({data.asset})</span></strong>}
      <small className={data.current ? styles.gridCurrent : undefined}>{data.source}</small>
    </span>
  );
}

function utcDate(value: string | null | undefined): string {
  if (!value) return "Not measured";
  const date = new Date(value.includes("T") ? value : `${value}T00:00:00Z`);
  return Number.isNaN(date.getTime()) ? value : date.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric", timeZone: "UTC" });
}

const percent =({ value }: { value: number | null }) => (value === null || value === undefined ? "Not measured" : `${value}%`);
const score = (field: keyof ComparisonRow, headerName: string, headerTooltip: string, higherIsBetter = true): ColDef<ComparisonRow> => ({
  field,
  headerName,
  headerTooltip,
  comparator: nullsLast,
  cellRenderer: ScoreCell,
  cellRendererParams: { higherIsBetter },
  minWidth: 104,
});
const measure = (field: keyof ComparisonRow, headerName: string, headerTooltip: string): ColDef<ComparisonRow> => ({
  field,
  headerName,
  headerTooltip,
  comparator: nullsLast,
  valueFormatter: percent,
  type: "rightAligned",
  minWidth: 96,
});

/**
 * Every asset this browser has analyzed, side by side. The figures are the paid results' own, unchanged:
 * the grid only sorts and lays them out, and says which analysis and day each row comes from.
 */
export default function AssetGrid({ rows, title }: { rows: ComparisonRow[]; title: string }) {
  const grid = useRef<AgGridReact<ComparisonRow>>(null);
  // On a phone the pinned column shows only the symbol, leaving room for the figures.
  const narrow = useSyncExternalStore(watchNarrow, () => window.matchMedia(NARROW).matches, () => false);
  const columns = useMemo<ColDef<ComparisonRow>[]>(() => [
    { field: "name", headerName: "Asset", pinned: "left", cellRenderer: AssetCell, cellRendererParams: { compact: narrow }, ...(narrow ? { width: 92, minWidth: 92, maxWidth: 92, flex: 0 } : { minWidth: 136 }), comparator: (left: string, right: string) => left.localeCompare(right) },
    { ...score("overallScore", "Overall", "Market standing reduced by measured price risk. Higher is better."), sort: "desc" },
    score("priceRisk", "Price risk", "From volatility and drawdowns over the past year. Higher is riskier.", false),
    measure("volatilityPct", "Volatility", "Annualized volatility of daily prices."),
    { ...measure("maxDrawdownPct", "Largest drop", "Largest fall from a high during the period."), minWidth: 124 },
    { ...score("marketStanding", "Market standing", "From trading volume, market size, and category."), minWidth: 146 },
    measure("belowHighPct", "Below high", "How far the last price was below the period's high."),
    score("liquidity", "Liquidity", "From trading volume and market size."),
    { field: "category", headerName: "Category", valueFormatter: ({ value }) => value ?? "Not screened", minWidth: 130 },
    { field: "measuredThrough", headerName: "Data through", minWidth: 120, valueFormatter: ({ value }) => utcDate(value) },
  ], [narrow]);

  return (
    <section className={styles.gridCard} aria-label={title}>
      <header className={styles.gridHeader}>
        <div>
          <h4>{title}</h4>
          <p>Tap a column to sort, and scroll sideways for more. Earlier rows are analyses you bought before, dated by their data.</p>
        </div>
        <button
          type="button"
          className={styles.textButton}
          onClick={() => grid.current?.api.exportDataAsCsv({
            fileName: "selun-asset-comparison.csv",
            processHeaderCallback: ({ column }) => `${column.getColDef().headerName ?? column.getColId()}${CSV_UNITS[column.getColId() as keyof ComparisonRow] ?? ""}`,
            processCellCallback: ({ value, column, node }) => {
              if (column.getColId() === "name" && node?.data) return `${node.data.name} (${node.data.asset})`;
              return value === null || value === undefined ? "" : String(value);
            },
          })}
        >
          Download CSV
        </button>
      </header>
      <div className={styles.gridFrame}>
        <AgGridReact<ComparisonRow>
          ref={grid}
          theme={theme}
          rowData={rows}
          columnDefs={columns}
          getRowId={({ data }) => data.key}
          defaultColDef={{ sortable: true, resizable: true, suppressMovable: true, flex: 1 }}
          rowClassRules={{ [styles.gridCurrentRow]: ({ data }) => Boolean(data?.current) }}
          domLayout="autoHeight"
          rowHeight={54}
          tooltipShowDelay={300}
          suppressCellFocus={false}
          ensureDomOrder
        />
      </div>
    </section>
  );
}
