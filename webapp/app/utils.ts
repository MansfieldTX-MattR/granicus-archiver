import 'server-only';

import type { ClipId, Clip, ClipResp } from "./model";
import { deserialize } from "./model";

interface ClipMainResp {
  base_dir: string;
  clip_ids: string[];
}

export interface ClipIndexResp {
  id: ClipId;
  location: string;
  name: string;
  datetime: string;
  data_file: string;
}

export interface ClipsIndexResp {
  clips: ClipIndexResp[];
  root_dir: string;
}

export const getTextData = async(path: string) => {
  const url = `http://localhost:8080/${path}`;
  const req = await fetch(url);
  return await req.text();
};

const getJsonData = async(path: string) => {
  const url = `http://localhost:8080/${path}`;
  const req = await fetch(url, {cache: 'no-cache'});
  return await req.json();
};

export const getItem = async(path: string) => {
  return await getTextData(path);
};

export const getClipData = async(clipId: ClipId): Promise<Clip> => {
  const data = await getJsonData(`clip/${clipId}`);
  return deserialize('Clip', data);
};

export const getClipsIndexData = async(): Promise<ClipIndexResp[]> => {
  const data: ClipsIndexResp = await getJsonData('data/clip-index.json');
  const clipsById: {[k: ClipId]: ClipIndexResp} = Object.fromEntries(data.clips.map((clip) => [clip.id, clip]));
  const clipIds = data.clips.map((clip) => parseInt(clip.id)).toSorted((a, b) => (a - b)).toReversed();
  const clips = clipIds.map((clipId) => clipsById[clipId.toString()]);
  return clips;
};

export const getClipIndexData = async(reqData: Pick<ClipIndexResp, 'id' | 'data_file'>): Promise<Clip> => {
  const data: ClipResp = await getJsonData(reqData.data_file);
  console.assert(data.parse_data.id === reqData.id, `data.parse_data.id "${data.parse_data.id}" !== reqData.id "${reqData.id}"`);
  return deserialize('Clip', data);
};

export const getClipRootData = async(): Promise<ClipMainResp> => {
  const data: ClipMainResp = await getJsonData('clips');
  return data;
}


// export const getMainData = async(): Promise<ClipCollection> => {
//   const dataStr = await getTextData('data/data.json');
//   return loadData(dataStr);
// };
