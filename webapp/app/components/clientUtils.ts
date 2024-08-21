'use client'
import { useState } from "react";

interface Formatter<Type> {
  locale: string;
  formatter: Type;
}

export const useDatetimeFormat = (locale?: string): Intl.DateTimeFormat => {
  const language = locale || navigator.language;
  const [ fmt, setFmt ] = useState<Formatter<Intl.DateTimeFormat>>(
    {locale: language, formatter: new Intl.DateTimeFormat(language)}
  );
  if (fmt.locale !== language) {
    setFmt({locale: language, formatter: new Intl.DateTimeFormat(language)});
  }
  return fmt.formatter;
};

export const useRelativeTimeFormat = (locale?: string): Intl.RelativeTimeFormat => {
  const language = locale || navigator.language;
  const [ fmt, setFmt ] = useState<Formatter<Intl.RelativeTimeFormat>>(
    {locale: language, formatter: new Intl.RelativeTimeFormat(language)}
  );
  if (fmt.locale !== language) {
    setFmt({locale: language, formatter: new Intl.RelativeTimeFormat(language)});
  }
  return fmt.formatter;
};

export const useRelativeTimeFormatter = (locale?: string) => {
  const formatter = useRelativeTimeFormat(locale);
  return (dt: Date|string|number, now?: Date) => {
    return formatDuration(formatter, dt, now);
  };
};



export const formatDuration = (formatter: Intl.RelativeTimeFormat, dt: Date|string|number, now?: Date) => {
  if (typeof dt === 'string' || typeof dt === 'number') {
    dt = new Date(dt);
  }
  if (now === undefined) {
    now = new Date();
  }
  const deltaSeconds = (now.valueOf() - dt.valueOf()) / 1000;
  let unit: Intl.RelativeTimeFormatUnit;
  let delta: number;
  if (deltaSeconds >= 86400) {
    delta = deltaSeconds / 86400;
    unit = 'day';
  } else if (deltaSeconds >= 3600) {
    delta = deltaSeconds / 3600;
    unit = 'hour';
  } else if (deltaSeconds >= 60) {
    delta = deltaSeconds / 60;
    unit = 'minute';
  } else {
    delta = deltaSeconds;
    unit = 'second';
  }
  delta = Math.round(delta);
  return formatter.format(-delta, unit);
};
