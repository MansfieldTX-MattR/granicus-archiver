import type { HasClassAndChildren } from "./types";
import { mergeClassNames } from "./utils";

type HeadingLevel = 1|2|3|4|5|6;

interface HeadingProps {
  level: HeadingLevel;
  title: string|JSX.Element;
  className?: string;
  headingBorder?: boolean;
}

interface SectionProps extends HasClassAndChildren {
  title?: string;
  level?: HeadingLevel;
  headingClass?: string;
}

const Heading = ({level, title, className, headingBorder}: HeadingProps) => {
  const hcls = mergeClassNames(
    className,
    // 'title',
    // headingBorder ? 'bordered' : undefined,
    // `is-${level}`,
    // className,
  );
  if (level === 1) return <h1 className={hcls}>{title}</h1>;
  if (level === 2) return <h2 className={hcls}>{title}</h2>;
  if (level === 3) return <h3 className={hcls}>{title}</h3>;
  if (level === 4) return <h4 className={hcls}>{title}</h4>;
  if (level === 5) return <h5 className={hcls}>{title}</h5>;
  if (level === 6) return <h6 className={hcls}>{title}</h6>;
  const e: never = level;
  return e;
};


export default function Section(props: SectionProps) {
  const h = props.title && props.level ?
    <Heading title={props.title} level={props.level} className={mergeClassNames(props.headingClass)} />
    : <></>;
  return (
    <section className={mergeClassNames("ml-5 my-4 prose dark:prose-invert", props.className)}>
      {h}
      {props.children}
    </section>
  );
}
