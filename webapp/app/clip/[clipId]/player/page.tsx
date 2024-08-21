"use server";
import { notFound } from "next/navigation";

import type { ClipId } from "@/app/model";
import { getClipsIndexData, getClipIndexData } from "@/app/utils";
import { Video } from "./clientComponents";

export default async function Page({params}: {params: {clipId: ClipId}}) {
  // const clip = await getClipData(params.clipId);
  const clips = await getClipsIndexData();
  const indexClips = clips.filter((c) => c.id === params.clipId);
  if (!indexClips.length) notFound();
  const indexClip = indexClips[0];
  // const clipRootData = await getClipRootData();
  const clip = await getClipIndexData(indexClip);

  return (
    // <Section title={clip.data.name} level={1}>
    <section className="mt-4 ml-3">
      <h1 className="text-4xl font-bold mb-3">{clip.data.name}</h1>
      <Video
        clip={clip}
        // className="w-fit self-center"
        // col1Class="w-max"
        col2Class="w-1/3"
      />
    </section>
  );
};
