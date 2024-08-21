
export interface HasClass {
  className?: string;
}

export interface HasChildren extends React.PropsWithChildren {}

export type HasClassAndChildren = HasClass & HasChildren;
