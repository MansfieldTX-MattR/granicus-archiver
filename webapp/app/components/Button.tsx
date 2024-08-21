import { Fragment } from "react";
import { Button } from "@headlessui/react";
import Link from "next/link";
import type { LinkProps } from "next/link";
import clsx from "clsx";

import { mergeClassNamesFiltered, mergeClassNames } from "./utils";

type ClickHandler<T> = React.MouseEventHandler<T>;

interface ButtonProps<ElemT> extends React.PropsWithChildren {
  className?: string;
  onClick?: ClickHandler<ElemT>;
}

interface LinkButtonProps extends LinkProps {
  className?: string;
  children?: React.ReactNode;
}

interface ToggleButtonProps extends ButtonProps<HTMLButtonElement> {
  selected?: boolean;
}

export const LinkButton = (props: LinkButtonProps) => {
  const overridePrefixes = ['px-', 'py-'];
  const overrideClasses = mergeClassNames(props.className).split(' ').filter((cls) => {
    const filtCls = overridePrefixes.filter((pfx) => cls.startsWith(pfx));
    return filtCls.length > 0;
  });
  const overriddenPrefixes = overrideClasses.map((cls) => cls.split('-')[0] + '-');
  const filteredClassNames = mergeClassNamesFiltered(
    [
      'rounded py-2 px-4 text-sm text-white',
      'bg-sky-950',
      'ring hover:ring-sky-500',
      'hover:bg-sky-800',
    ],
    {
      filterFunc: (cls) => {
        const overridden = overriddenPrefixes.filter((pfx) => cls.startsWith(pfx));
        return overridden.length === 0;
      }
    }
  );
  const classNames = mergeClassNames(props.className, filteredClassNames);
  return (
    <Button as={Fragment}>
      <Link {...props} className={classNames} />
    </Button>
  );
};

export const ToggleButton = (
  {className, selected, onClick, children}: ToggleButtonProps
) => {
  return (
    <Button as={Fragment}>
      {({hover}) => (
        <button
          onClick={onClick}
          className={clsx(
            'rounded py-2 px-4 text-sm text-white',
            !hover && !selected && 'bg-sky-950',
            hover && !selected && 'bg-sky-800',
            selected && 'outline-none ring ring-sky-500',
            hover && selected && 'bg-sky-700',
            !hover && selected && 'bg-sky-800',
            className,
          )}
        >
          {children}
        </button>
      )}
    </Button>
  );
}
