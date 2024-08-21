export type OptionalString = string|undefined;

export function ensureString(s?: string): string {
  return s ? s : "";
}

function classNamesToList(...className: OptionalString[]): string[] {
  const l = className.map((cls) => ensureString(cls).split(' ')).filter((cls) => cls.length > 0).flat();
  const clsSet = new Set();

  return l.filter((cls) => {
    if (clsSet.has(cls)) return false;
    clsSet.add(cls);
    return true;
  });
}

export function mergeClassNames(...className: OptionalString[]): string {
  const classNames = classNamesToList(...className);
  return classNames.join(" ");
}

export function mergeClassNamesFiltered(
  className: OptionalString[],
  {filterClasses, filterFunc}: {filterClasses?: string[], filterFunc?: (cls: string) => boolean}
): string {
  const classNames = classNamesToList(...className);
  const filtered = classNames.filter((cls) => {
    if (filterFunc !== undefined) return filterFunc(cls);
    if (filterClasses !== undefined && cls in filterClasses) {
      return false;
    }
    return true;
  });
  return filtered.join(" ");
};

export function ensureClassName(requiredClass: string, ...className: OptionalString[]) {
  const classNames = classNamesToList(...className);
  if (classNames.indexOf(requiredClass) === -1){
    classNames.push(requiredClass);
  }
  return classNames.join(" ");
}

const zeroPad = (n: number, count: number = 2): string => {
  const s = n.toString();
  return s.padStart(count, '0');
};

export const formatDurationDelta = (deltaSeconds: number) => {
  const h = Math.floor(deltaSeconds / 3600);
  const m = Math.floor((deltaSeconds - h * 3600) / 60);
  const s = deltaSeconds % 60;
  return `${zeroPad(h)}:${zeroPad(m)}:${zeroPad(s)}`;
};
