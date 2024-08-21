// import clsx from "clsx";

import type { HasClassAndChildren } from "./types";
import { mergeClassNames } from "./utils";


type FlexDirection = 'column' | 'row';
type FlexAlignBase = 'flex-start' | 'flex-end' | 'center' | 'space-between' | 'space-around' | 'space-evenly' | 'start' | 'end';
type FlexJustifyContent = FlexAlignBase | 'left' | 'right';
type FlexAlignContent = FlexAlignBase | 'stretch' | 'baseline';
type FlexAlignItems = FlexAlignBase | 'stretch' | 'baseline' | 'self-start' | 'self-end';
type FlexAlignSelf = 'auto' | 'flex-start' | 'flex-end' | 'center' | 'baseline' | 'stretch';


interface FlexAlignParams {
  justifyContent?: FlexJustifyContent;
  alignItems?: FlexAlignItems;
  alignContent?: FlexAlignContent;
  alignSelf?: FlexAlignSelf;
};


interface FlexBoxParams extends FlexAlignParams {
  direction: FlexDirection;
};

export type FlexBoxProps = FlexBoxParams & HasClassAndChildren;
export type FlexRowProps = FlexAlignParams & HasClassAndChildren;
export type FlexColumnProps = FlexAlignParams & HasClassAndChildren;


function buildFlexCls(props: FlexAlignParams & {direction?: FlexDirection}): string[] {
  function splitLast(s: string, delim: string = '-'): string {
    if (!s.includes(delim)) return s;
    return s.split(delim).slice(-1)[0];
  }
  const c = [
    'flex',
    props.direction === 'row' ? 'flex-row' : props.direction === 'column' ? 'flex-col' : undefined,
    props.justifyContent ? `justify-${splitLast(props.justifyContent)}` : undefined,
    props.alignContent ? `content-${splitLast(props.alignContent)}` : undefined,
    props.alignItems ? `items-${splitLast(props.alignItems)}` : undefined,
    props.alignSelf ? `self-${splitLast(props.alignSelf)}` : undefined,
  ];
  return c.filter((s) => s !== undefined) as string[];
}

export const FlexBox = (props: FlexBoxProps) => {
  const className = mergeClassNames(props.className, ...buildFlexCls(props));
  return (
    <div className={className}>{props.children}</div>
  );
};

export const FlexRow = (props: FlexRowProps) => {
  const className = mergeClassNames(
    props.className,
    ...buildFlexCls(props),
    ...buildFlexCls({direction:'row'})
  );
  return (
    <div className={className}>{props.children}</div>
  );
};

export const FlexColumn = (props: FlexColumnProps) => {
  const className = mergeClassNames(
    props.className,
    ...buildFlexCls(props),
    ...buildFlexCls({direction:'column'})
  );
  return (
    <div className={className}>{props.children}</div>
  );
};
