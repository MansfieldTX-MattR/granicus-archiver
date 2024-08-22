'use server';
import { notFound } from "next/navigation";

import type { ClipId, Clip, ClipFileKey } from "@/app/model";
import { getClipsIndexData, getClipIndexData } from "@/app/utils";
import Section from "@/app/components/Section";
import { FlexColumn } from "@/app/components/FlexBox";
import { LinkButton } from "@/app/components/Button";
import { formatDurationDelta } from "@/app/components/utils";
import { getUrl } from "@/app/utils";


export default async function Page({params}: {params: {clipId: ClipId}}) {
  // const clip = await getClipData(params.clipId);
  const clips = await getClipsIndexData();
  const indexClips = clips.filter((c) => c.id === params.clipId);
  if (!indexClips.length) notFound();
  const indexClip = indexClips[0];
  // const clipRootData = await getClipRootData();
  const clip = await getClipIndexData(indexClip);

  return (
    // <Video clip={clip} />
    // <FlexColumn className="w-full">
    <Section title={`${clip.data.name} - ${clip.data.datetime.toLocaleDateString()}`} level={1}>
      {/* <Link className="text-xl bg-sky-700 px-4 py-2 border rounded-md no-underline" href={`${clip.id}/player`}>Watch</Link> */}

      <ClipInfo clip={clip} />
      {/* <Video clip={clip} /> */}
      {/* <FlexRow>
        <ClipInfo clip={clip} />
      </FlexRow> */}
    </Section>

  );
}

const ClipInfo = ({clip}: {clip: Clip}) => {
  // const formatter = useRelativeTimeFormat();
  // const epoch = new Date(0);
  const linkKeys: ClipFileKey[] = ['video', 'audio', 'agenda', 'minutes'];
  const linkDisplays = linkKeys.map((key) => {
    const fileName = clip.files[key];
    // if (!fileName) return <></>;
    const link = fileName ? <a href={getUrl(fileName).toString()}>Download</a> : <span className="text-slate-500">unavailable</span>
    return (
      <InfoDisplay key={key} title={key} value={link} />
    );
  });
  return (
    <div className="grid grid-flow-row grid-cols-2 grid-rows-5 gap-x-5 mt-4">
      <InfoDisplay title="Date" value={clip.data.datetime.toLocaleDateString()} />
      <InfoDisplay title="Time" value={clip.data.datetime.toLocaleTimeString()} />
      <InfoDisplay title="Duration" value={formatDurationDelta(clip.data.duration)} />
      <InfoDisplay title="Folder" value={clip.data.location} />
      {linkDisplays}
      <div className="py-2 px-2 my-2 col-span-full flex flex-row justify-center align-center content-center">
          <LinkButton className="mt-auto px-5 text-xl no-underline align-middle" href={`${clip.id}/player`}>Watch</LinkButton>
      </div>

    </div>
  );
};

interface InfoDisplayProps {
  title: string;
  value: string|JSX.Element;
}

const InfoDisplay = ({title, value}: InfoDisplayProps) => {
  // return (
  //   <dl className="border border-slate-700 rounded-md px-2">
  //     <dt className="capitalize">{title}</dt>
  //     <dd>{value}</dd>
  //   </dl>
  // )
  return (
    // <div className="border border-slate-700 rounded-md px-2">
    <FlexColumn className="border border-slate-700 rounded-md py-2 px-2 my-2">
      <div className="capitalize">{title}</div>
      <div className="px-4">{value}</div>
    </FlexColumn>
  );
};
