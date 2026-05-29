import { useState, useEffect, useMemo } from "react";
import { useTranslation } from "react-i18next";
import {
  ChevronRight,
  ChevronLeft,
  ArrowRight,
  AlertTriangle,
  Play,
  HelpCircle,
  FolderSync,
  Database,
  HardDrive,
  SlidersHorizontal,
} from "lucide-react";
import { toast } from "sonner";

import {
  useConfig,
  useCreateFlow,
  useDiscoverSourceTables,
  useDiscoverSinkTables,
} from "@/lib/query/manager";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Switch } from "@/components/ui/switch";
import { Badge } from "@/components/ui/badge";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { isTypeCompatible } from "@/lib/typecompat";
import type { ColumnMapping } from "@/types/api";

interface FlowWizardProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

const UNMAPPED_COLUMN_VALUE = "__unmapped__";

export function FlowWizard({ open, onOpenChange }: FlowWizardProps) {
  const { t } = useTranslation();
  const [step, setStep] = useState(1);

  // Form State
  const [flowName, setFlowName] = useState("");
  const [selectedSourceId, setSelectedSourceId] = useState("");
  const [selectedSinkId, setSelectedSinkId] = useState("");
  const [selectedSourceTable, setSelectedSourceTable] = useState("");
  const [selectedSinkTable, setSelectedSinkTable] = useState("");
  const [columnMappings, setColumnMappings] = useState<ColumnMapping[]>([]);
  const [batchSize, setBatchSize] = useState(100);
  const [flushIntervalMs, setFlushIntervalMs] = useState(1000);
  const [partitionCount, setPartitionCount] = useState(4);
  const [filterExpression, setFilterExpression] = useState("");

  // Queries
  const { data: configData } = useConfig();
  const createFlowMutation = useCreateFlow();

  const { data: sourceTablesData, isLoading: sourceTablesLoading } =
    useDiscoverSourceTables(selectedSourceId);
  const { data: sinkTablesData, isLoading: sinkTablesLoading } =
    useDiscoverSinkTables(selectedSinkId);

  // Sync types & names
  const selectedSource = useMemo(() => {
    return configData?.config?.sources?.find(
      (s) => s.instance_id === selectedSourceId,
    );
  }, [configData, selectedSourceId]);

  const selectedSink = useMemo(() => {
    return configData?.config?.sinks?.find(
      (s) => s.instance_id === selectedSinkId,
    );
  }, [configData, selectedSinkId]);

  // Find column info for selected tables
  const tableFullName = (schema: string, name: string) => `${schema}.${name}`;

  const sourceTableColumns = useMemo(() => {
    if (!sourceTablesData?.tables || !selectedSourceTable) return [];
    const tInfo = sourceTablesData.tables.find(
      (t) =>
        `${t.schema}.${t.name}` === selectedSourceTable ||
        t.name === selectedSourceTable,
    );
    return tInfo?.columns || [];
  }, [sourceTablesData, selectedSourceTable]);

  const sinkTableColumns = useMemo(() => {
    if (!sinkTablesData?.tables || !selectedSinkTable) return [];
    const tInfo = sinkTablesData.tables.find(
      (t) =>
        `${t.schema}.${t.name}` === selectedSinkTable ||
        t.name === selectedSinkTable,
    );
    return tInfo?.columns || [];
  }, [sinkTablesData, selectedSinkTable]);

  useEffect(() => {
    setSelectedSourceTable("");
  }, [selectedSourceId]);

  useEffect(() => {
    setSelectedSinkTable("");
  }, [selectedSinkId]);

  // Initialize Column Mappings
  useEffect(() => {
    if (sourceTableColumns.length > 0) {
      // Auto mapping by name
      const mappings: ColumnMapping[] = sourceTableColumns.map((srcCol) => {
        // Find matching column in sink table
        const matchingSinkCol = sinkTableColumns.find(
          (sc) => sc.name.toLowerCase() === srcCol.name.toLowerCase(),
        );

        return {
          source_column: srcCol.name,
          sink_column: matchingSinkCol ? matchingSinkCol.name : "",
          source_type: srcCol.type,
          sink_type: matchingSinkCol ? matchingSinkCol.type : "",
          enabled: !!matchingSinkCol,
        };
      });
      setColumnMappings(mappings);
    } else {
      setColumnMappings([]);
    }
  }, [sourceTableColumns, sinkTableColumns]);

  // Validate current step
  const isStepValid = useMemo(() => {
    if (step === 1) {
      return !!selectedSourceId && !!selectedSinkId;
    }
    if (step === 2) {
      return !!selectedSourceTable && !!selectedSinkTable;
    }
    if (step === 3) {
      const enabledMappings = columnMappings.filter((m) => m.enabled);
      return (
        enabledMappings.length > 0 &&
        enabledMappings.every((m) => !!m.sink_column && !!m.sink_type)
      );
    }
    return true;
  }, [
    step,
    selectedSourceId,
    selectedSinkId,
    selectedSourceTable,
    selectedSinkTable,
    columnMappings,
  ]);

  // Reset form helper
  const resetForm = () => {
    setStep(1);
    setFlowName("");
    setSelectedSourceId("");
    setSelectedSinkId("");
    setSelectedSourceTable("");
    setSelectedSinkTable("");
    setColumnMappings([]);
    setBatchSize(100);
    setFlushIntervalMs(1000);
    setPartitionCount(4);
    setFilterExpression("");
  };

  const handleNext = () => {
    if (isStepValid) setStep((s) => s + 1);
  };

  const handleBack = () => {
    setStep((s) => Math.max(1, s - 1));
  };

  const handleCreate = async () => {
    try {
      // Collect mappings
      const enabledMappings = columnMappings.filter((m) => m.enabled);

      await createFlowMutation.mutateAsync({
        ...(flowName.trim() ? { name: flowName.trim() } : {}),
        source_id: selectedSourceId,
        sink_id: selectedSinkId,
        source_table: selectedSourceTable,
        sink_table: selectedSinkTable,
        column_mappings: enabledMappings,
        options: {
          batch_size: batchSize,
          flush_interval_ms: flushIntervalMs,
          partition_count: partitionCount,
          filter_expression: filterExpression || undefined,
        },
      });

      toast.success(t("common.success"));
      resetForm();
      onOpenChange(false);
    } catch (err) {
      toast.error(err instanceof Error ? err.message : t("manager.flows.createFailed"));
    }
  };

  // Determine if there are incompatible mappings
  const incompatibleCount = useMemo(() => {
    const sinkConnectorType = selectedSink?.type || "stdout";
    return columnMappings.filter(
      (m) =>
        m.enabled &&
        !isTypeCompatible(sinkConnectorType, m.source_type, m.sink_type),
    ).length;
  }, [columnMappings, selectedSink]);

  return (
    <Dialog
      open={open}
      onOpenChange={(val) => {
        onOpenChange(val);
        if (!val) resetForm();
      }}
    >
      <DialogContent className="min-w-3xl overflow-hidden p-0">
        <DialogHeader className="border-b border-border bg-muted/20 px-6 py-5">
          <div className="flex items-start gap-3">
            <div className="flex h-11 w-11 shrink-0 items-center justify-center rounded-lg border border-amber-500/25 bg-amber-500/10">
              <FolderSync className="h-5 w-5 text-amber-500" />
            </div>
            <div className="min-w-0">
              <DialogTitle className="text-xl font-semibold tracking-tight text-foreground">
                {t("manager.flows.create")}
              </DialogTitle>
              <DialogDescription className="mt-1 max-w-xl text-sm text-muted-foreground">
                {t("manager.flows.createDesc")}
              </DialogDescription>
            </div>
          </div>
        </DialogHeader>

        {/* Wizard Steps indicator */}
        <div className="mx-6 mt-5 grid grid-cols-4 overflow-hidden rounded-lg border border-border bg-card text-xs font-semibold select-none">
          {[1, 2, 3, 4].map((s) => (
            <div
              key={s}
              className={`flex min-w-0 items-center gap-2 border-r border-border px-3 py-3 last:border-r-0 ${
                step === s
                  ? "bg-amber-500/10"
                  : step > s
                    ? "bg-emerald-500/5"
                    : ""
              }`}
            >
              <span
                className={`flex h-6 w-6 shrink-0 items-center justify-center rounded-full text-[10px] ${
                  step === s
                    ? "bg-amber-500 text-black font-bold"
                    : step > s
                      ? "bg-emerald-500/10 text-emerald-500 border border-emerald-500/25"
                      : "bg-muted text-muted-foreground border border-border"
                }`}
              >
                {s}
              </span>
              <span
                className={`truncate ${step === s ? "text-foreground" : "text-muted-foreground"}`}
              >
                {s === 1 && t("manager.flows.steps.connectors")}
                {s === 2 && t("manager.flows.steps.tables")}
                {s === 3 && t("manager.flows.steps.columns")}
                {s === 4 && t("manager.flows.steps.options")}
              </span>
            </div>
          ))}
        </div>

        {/* Step Contents */}
        <div className="min-h-[380px] max-h-[520px] overflow-y-auto px-6 py-5">
          {/* STEP 1: Connectors selection */}
          {step === 1 && (
            <div className="space-y-5">
              <div>
                <label className="text-xs font-semibold text-muted-foreground mb-2 block">
                  {t("manager.flows.fields.selectSource")}
                </label>
                <Select
                  value={selectedSourceId}
                  onValueChange={(val) => setSelectedSourceId(val || "")}
                >
                  <SelectTrigger className="w-full h-10 text-xs">
                    <SelectValue
                      placeholder={t("manager.flows.placeholders.chooseSource")}
                    />
                  </SelectTrigger>
                  <SelectContent>
                    {configData?.config?.sources?.map((s) => (
                      <SelectItem
                        key={s.instance_id}
                        value={s.instance_id}
                        className="text-xs"
                      >
                        {s.name || s.instance_id} ({s.type}) - {s.host}:{s.port}
                        /{s.database}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>

              <div>
                <label className="text-xs font-semibold text-muted-foreground mb-2 block">
                  {t("manager.flows.fields.selectSink")}
                </label>
                <Select
                  value={selectedSinkId}
                  onValueChange={(val) => setSelectedSinkId(val || "")}
                >
                  <SelectTrigger className="w-full h-10 text-xs">
                    <SelectValue
                      placeholder={t("manager.flows.placeholders.chooseSink")}
                    />
                  </SelectTrigger>
                  <SelectContent>
                    {configData?.config?.sinks?.map((s) => (
                      <SelectItem
                        key={s.instance_id}
                        value={s.instance_id}
                        className="text-xs"
                      >
                        {s.name || s.instance_id} ({s.type})
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              </div>
            </div>
          )}

          {/* STEP 2: Table mappings */}
          {step === 2 && (
            <div className="space-y-5">
              <div className="grid grid-cols-2 gap-4">
                {/* Source Table */}
                <div>
                  <label className="text-xs font-semibold text-muted-foreground mb-2 block">
                    {t("manager.flows.fields.sourceTable")}
                  </label>
                  {sourceTablesLoading ? (
                    <div className="h-10 w-full bg-muted animate-pulse rounded-lg border border-border" />
                  ) : (
                    <Select
                      value={selectedSourceTable}
                      onValueChange={(val) => setSelectedSourceTable(val || "")}
                    >
                      <SelectTrigger className="w-full h-10 text-xs">
                        <SelectValue
                          placeholder={t(
                            "manager.flows.placeholders.chooseTable",
                          )}
                        />
                      </SelectTrigger>
                      <SelectContent>
                        {sourceTablesData?.tables?.map((tInfo) => {
                          const fullName = tableFullName(
                            tInfo.schema,
                            tInfo.name,
                          );
                          return (
                            <SelectItem
                              key={fullName}
                              value={fullName}
                              className="text-xs"
                            >
                              {fullName}
                            </SelectItem>
                          );
                        })}
                      </SelectContent>
                    </Select>
                  )}
                </div>

                {/* Sink Table */}
                <div>
                  <label className="text-xs font-semibold text-muted-foreground mb-2 block">
                    {t("manager.flows.fields.targetTableIndex")}
                  </label>
                  {selectedSink?.type === "elasticsearch" ? (
                    <Input
                      value={selectedSinkTable}
                      onChange={(e) => setSelectedSinkTable(e.target.value)}
                      placeholder={t("manager.flows.placeholders.targetIndex")}
                      className="h-10 text-xs"
                    />
                  ) : sinkTablesLoading ? (
                    <div className="h-10 w-full bg-muted animate-pulse rounded-lg border border-border" />
                  ) : (
                    <Select
                      value={selectedSinkTable}
                      onValueChange={(val) => setSelectedSinkTable(val || "")}
                    >
                      <SelectTrigger className="w-full h-10 text-xs">
                        <SelectValue
                          placeholder={t(
                            "manager.flows.placeholders.chooseTable",
                          )}
                        />
                      </SelectTrigger>
                      <SelectContent>
                        {sinkTablesData?.tables?.map((tInfo) => {
                          const fullName = tableFullName(
                            tInfo.schema,
                            tInfo.name,
                          );
                          return (
                            <SelectItem
                              key={fullName}
                              value={fullName}
                              className="text-xs"
                            >
                              {fullName}
                            </SelectItem>
                          );
                        })}
                      </SelectContent>
                    </Select>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* STEP 3: Column Mappings */}
          {step === 3 && (
            <div className="space-y-4">
              {incompatibleCount > 0 && (
                <div className="rounded-xl border border-yellow-500/20 bg-yellow-500/5 p-4 flex gap-3 text-xs text-yellow-400">
                  <AlertTriangle className="h-4 w-4 shrink-0 text-yellow-400 mt-0.5" />
                  <div>
                    <span className="font-semibold text-yellow-300">
                      {t("manager.flows.validation.incompatible")}
                    </span>
                    <p className="opacity-95 text-[11px] leading-relaxed mt-0.5">
                      {t("manager.flows.validation.warningDesc")}
                    </p>
                  </div>
                </div>
              )}

              {/* Columns Table */}
              <div className="rounded-lg border border-border bg-card overflow-hidden text-xs">
                <Table>
                  <TableHeader className="bg-muted/50 border-b border-border text-muted-foreground select-none font-semibold">
                    <TableRow>
                      <TableHead className="px-4 py-2 w-10">
                        {t("manager.flows.table.active")}
                      </TableHead>
                      <TableHead className="px-4 py-2">
                        {t("manager.flows.table.sourceColumn")}
                      </TableHead>
                      <TableHead className="px-4 py-2 w-8 text-center"></TableHead>
                      <TableHead className="px-4 py-2">
                        {t("manager.flows.table.sinkColumn")}
                      </TableHead>
                      <TableHead className="px-4 py-2 w-28">
                        {t("manager.flows.table.validation")}
                      </TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody className="divide-y divide-border">
                    {columnMappings.map((m, idx) => {
                      const sinkConnectorType = selectedSink?.type || "stdout";
                      const compatible = isTypeCompatible(
                        sinkConnectorType,
                        m.source_type,
                        m.sink_type,
                      );

                      const updateMapping = (
                        key: keyof ColumnMapping,
                        val: ColumnMapping[keyof ColumnMapping],
                      ) => {
                        const next = [...columnMappings];
                        next[idx] = { ...next[idx], [key]: val };
                        setColumnMappings(next);
                      };

                      const selectSinkColumn = (columnName: string | null) => {
                        if (!columnName) {
                          return;
                        }
                        if (columnName === UNMAPPED_COLUMN_VALUE) {
                          const next = [...columnMappings];
                          next[idx] = {
                            ...next[idx],
                            sink_column: "",
                            sink_type: "",
                            enabled: false,
                          };
                          setColumnMappings(next);
                          return;
                        }

                        const sinkColumn = sinkTableColumns.find(
                          (col) => col.name === columnName,
                        );
                        const next = [...columnMappings];
                        next[idx] = {
                          ...next[idx],
                          sink_column: columnName,
                          sink_type: sinkColumn?.type || "",
                          enabled: true,
                        };
                        setColumnMappings(next);
                      };

                      return (
                        <TableRow
                          key={m.source_column}
                          className={`hover:bg-muted/50 ${!m.enabled ? "opacity-40" : ""}`}
                        >
                          <TableCell className="px-4 py-3">
                            <Switch
                              checked={m.enabled}
                              onCheckedChange={(val) =>
                                updateMapping("enabled", val)
                              }
                              className="h-4 w-7"
                            />
                          </TableCell>
                          <TableCell className="px-4 py-3">
                            <div className="font-mono font-medium text-foreground">
                              {m.source_column}
                            </div>
                            <div className="text-[10px] text-muted-foreground font-mono mt-0.5">
                              {m.source_type}
                            </div>
                          </TableCell>
                          <TableCell className="px-4 py-3 text-center">
                            <ArrowRight className="h-3.5 w-3.5 text-muted-foreground inline" />
                          </TableCell>
                          <TableCell className="px-4 py-3">
                            <Select
                              value={
                                m.sink_column || UNMAPPED_COLUMN_VALUE
                              }
                              onValueChange={selectSinkColumn}
                              disabled={sinkTableColumns.length === 0}
                            >
                              <SelectTrigger className="h-8 max-w-[220px] font-mono text-xs">
                                <SelectValue
                                  placeholder={t(
                                    "manager.flows.placeholders.chooseColumn",
                                  )}
                                />
                              </SelectTrigger>
                              <SelectContent>
                                <SelectItem
                                  value={UNMAPPED_COLUMN_VALUE}
                                  className="text-xs text-muted-foreground"
                                >
                                  {t("manager.flows.placeholders.noTargetColumn")}
                                </SelectItem>
                                {sinkTableColumns.map((column) => (
                                  <SelectItem
                                    key={column.name}
                                    value={column.name}
                                    className="text-xs"
                                  >
                                    <span className="font-mono">
                                      {column.name}
                                    </span>
                                    <span className="ml-2 text-[10px] text-muted-foreground">
                                      {column.type}
                                    </span>
                                  </SelectItem>
                                ))}
                              </SelectContent>
                            </Select>
                            <div className="mt-1 min-h-4 font-mono text-[10px] text-muted-foreground">
                              {m.sink_type ||
                                t("manager.flows.validation.notMapped")}
                            </div>
                          </TableCell>
                          <TableCell className="px-4 py-3">
                            {m.enabled ? (
                              compatible ? (
                                <span className="inline-flex items-center gap-1 text-[10px] font-semibold text-emerald-400">
                                  {t("manager.flows.validation.compatible")}
                                </span>
                              ) : (
                                <span
                                  className="inline-flex items-center gap-1 text-[10px] font-semibold text-yellow-500 cursor-help"
                                  title={t(
                                    "manager.flows.validation.typeMismatch",
                                    {
                                      srcType: m.source_type,
                                      sinkType: m.sink_type,
                                    },
                                  )}
                                >
                                  <AlertTriangle className="h-3 w-3 shrink-0 text-yellow-500" />
                                  {t("manager.flows.validation.warning")}
                                </span>
                              )
                            ) : (
                              <span className="text-[10px] text-muted-foreground font-semibold italic">
                                {t("manager.flows.validation.disabled")}
                              </span>
                            )}
                          </TableCell>
                        </TableRow>
                      );
                    })}
                  </TableBody>
                </Table>
              </div>
            </div>
          )}

          {/* STEP 4: Advanced Options */}
          {step === 4 && (
            <div className="space-y-5">
              <div className="grid gap-3 md:grid-cols-2">
                <div className="rounded-lg border border-sky-500/20 bg-sky-500/5 p-4">
                  <div className="flex items-center gap-2 text-[11px] font-semibold uppercase tracking-wide text-sky-600 dark:text-sky-300">
                    <Database className="h-4 w-4" />
                    {t("nav.sources")}
                  </div>
                  <div className="mt-3 truncate text-sm font-semibold text-foreground">
                    {selectedSource?.name ||
                      selectedSource?.database ||
                      selectedSourceId}
                  </div>
                  <div className="mt-1 truncate font-mono text-xs text-muted-foreground">
                    {selectedSourceTable}
                  </div>
                </div>
                <div className="rounded-lg border border-violet-500/20 bg-violet-500/5 p-4">
                  <div className="flex items-center gap-2 text-[11px] font-semibold uppercase tracking-wide text-violet-600 dark:text-violet-300">
                    <HardDrive className="h-4 w-4" />
                    {t("nav.sinks")}
                  </div>
                  <div className="mt-3 truncate text-sm font-semibold text-foreground">
                    {selectedSink?.name ||
                      selectedSink?.database ||
                      selectedSinkId}
                  </div>
                  <div className="mt-1 truncate font-mono text-xs text-muted-foreground">
                    {selectedSinkTable}
                  </div>
                </div>
              </div>

              {/* Flow name */}
              <div className="rounded-lg border border-border bg-card p-4">
                <div className="mb-3 flex items-center justify-between gap-3">
                  <label className="text-xs font-semibold text-muted-foreground">
                    {t("manager.flows.fields.flowName")}
                  </label>
                  <Badge
                    variant="outline"
                    className="border-emerald-500/25 bg-emerald-500/10 text-emerald-600 dark:text-emerald-400"
                  >
                    {t("common.optional")}
                  </Badge>
                </div>
                <Input
                  value={flowName}
                  onChange={(e) => setFlowName(e.target.value)}
                  className="h-11 font-mono text-sm"
                />
              </div>

              {/* Sync Rate Options */}
              <div className="rounded-lg border border-border bg-card p-4">
                <div className="mb-4 flex items-center gap-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                  <SlidersHorizontal className="h-4 w-4" />
                  {t("manager.flows.options")}
                </div>
                <div className="grid grid-cols-3 gap-4">
                  <div>
                    <label className="text-xs font-semibold text-muted-foreground mb-2 block">
                      {t("manager.flows.fields.batchSize")}
                    </label>
                    <Input
                      type="number"
                      value={batchSize}
                      onChange={(e) => setBatchSize(Number(e.target.value))}
                      className="h-10 text-xs"
                    />
                  </div>
                  <div>
                    <label className="text-xs font-semibold text-muted-foreground mb-2 block">
                      {t("manager.flows.fields.flushInterval")}
                    </label>
                    <Input
                      type="number"
                      value={flushIntervalMs}
                      onChange={(e) =>
                        setFlushIntervalMs(Number(e.target.value))
                      }
                      className="h-10 text-xs"
                    />
                  </div>
                  <div>
                    <label className="text-xs font-semibold text-muted-foreground mb-2 block">
                      {t("manager.flows.fields.partitionCount")}
                    </label>
                    <Input
                      type="number"
                      min={1}
                      value={partitionCount}
                      onChange={(e) =>
                        setPartitionCount(Number(e.target.value))
                      }
                      className="h-10 text-xs"
                    />
                  </div>
                </div>
              </div>

              {/* Filter Expression */}
              <div className="rounded-lg border border-border bg-card p-4">
                <label className="text-xs font-semibold text-muted-foreground mb-2 block flex items-center gap-1.5">
                  {t("manager.flows.fields.filterExpression")}
                  <span
                    className="cursor-help"
                    title={t("manager.flows.tooltips.filterExpression")}
                  >
                    <HelpCircle className="h-3.5 w-3.5 text-muted-foreground" />
                  </span>
                </label>
                <Input
                  value={filterExpression}
                  onChange={(e) => setFilterExpression(e.target.value)}
                  placeholder={t("manager.flows.placeholders.filterExpression")}
                  className="h-10 text-xs font-mono"
                />
              </div>
            </div>
          )}
        </div>

        {/* Dialog Actions */}
        <div className="flex items-center justify-between border-t border-border bg-muted/20 px-6 py-4">
          <Button
            variant="outline"
            onClick={step === 1 ? () => onOpenChange(false) : handleBack}
            className="h-9 text-xs cursor-pointer"
          >
            {step === 1 ? (
              t("common.cancel")
            ) : (
              <>
                <ChevronLeft className="h-4 w-4 mr-1" /> {t("common.previous")}
              </>
            )}
          </Button>

          {step < 4 ? (
            <Button
              onClick={handleNext}
              disabled={!isStepValid}
              className="h-9 text-xs bg-amber-500 text-black hover:bg-amber-400 font-semibold cursor-pointer"
            >
              {t("common.continue")}
              <ChevronRight className="h-4 w-4 ml-1" />
            </Button>
          ) : (
            <Button
              onClick={handleCreate}
              disabled={createFlowMutation.isPending}
              className="h-9 text-xs bg-amber-500 text-black hover:bg-amber-400 font-semibold cursor-pointer"
            >
              {createFlowMutation.isPending ? (
                t("manager.flows.creating")
              ) : (
                <>
                  <Play className="h-3.5 w-3.5 mr-1" />{" "}
                  {t("manager.flows.start")}
                </>
              )}
            </Button>
          )}
        </div>
      </DialogContent>
    </Dialog>
  );
}
