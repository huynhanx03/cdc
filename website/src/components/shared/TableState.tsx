import type { ReactNode } from 'react';

import { TableCell, TableRow } from '@/components/ui/table';

interface EmptyTableRowProps {
  colSpan: number;
  children: ReactNode;
}

interface LoadingTableRowsProps {
  colSpan: number;
  rows?: number;
}

export function EmptyTableRow({ colSpan, children }: EmptyTableRowProps) {
  return (
    <TableRow>
      <TableCell colSpan={colSpan} className="h-32 text-center text-sm text-muted-foreground">
        {children}
      </TableCell>
    </TableRow>
  );
}

export function LoadingTableRows({ colSpan, rows = 5 }: LoadingTableRowsProps) {
  return Array.from({ length: rows }).map((_, index) => (
    <TableRow key={index}>
      <TableCell colSpan={colSpan}>
        <div className="h-6 animate-pulse rounded bg-muted" />
      </TableCell>
    </TableRow>
  ));
}
