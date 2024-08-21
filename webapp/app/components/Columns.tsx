// import clsx from "clsx";

import type { HasClassAndChildren } from "./types";
import type { FlexColumnProps, FlexRowProps } from "./FlexBox";
import { FlexColumn, FlexRow } from "./FlexBox";
import { mergeClassNames } from "./utils";

// type ColumnsProps = {
//   numCols?: number;
// } & FlexRowProps & HasClassAndChildren;
interface ColumnsProps extends HasClassAndChildren {
  numCols?: number;
  flex?: Omit<FlexRowProps, 'children' | 'className'>;
}

type ColumnProps = FlexColumnProps & HasClassAndChildren;


export const Columns = (props: ColumnsProps) => {
  const colCls = mergeClassNames(
    'grid',
    props.numCols ? `grid-cols-${props.numCols}` : 'grid-flow-col auto-cols-auto',
    // 'w-full',
    props.className,
  );
  const flexProps = props.flex || {};
  const flexCls = undefined// 'w-full';

  return (
    <div className={colCls}>
      {props.children}
      {/* <FlexRow className={flexCls} {...flexProps}>
        {props.children}
      </FlexRow> */}
    </div>
  );
};

export const Column = (props: ColumnProps) => {
  return (
    <FlexColumn {...props} />
  );
};
