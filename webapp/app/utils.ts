import 'server-only';

import type { ClipId, Clip, ClipResp } from "./model";
import { deserialize } from "./model";

const HOST_PATH = process.env.CONTENT_HOST_PATH;

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


interface FetchErrorPayload {
  aborted?: boolean;
  url: URL|string;
  status: number;
  statusText: string;
}

class FetchError extends Error {
  readonly payload: FetchErrorPayload;
  constructor(payload: FetchErrorPayload) {
    const msg = payload.aborted ? 'Connection aborted' : `${payload.status} - ${payload.statusText}`;
    super(msg);
    this.name = 'FetchError';
    this.payload = payload;
  }
}

export const getTextData = async(path: string) => {
  const url = getUrl(path);
  const req = await fetch(url);
  return await req.text();
};


const getFetchResp = async(url: URL|string) => {
  try {
    return await fetch(url, {cache: 'no-cache'});
  } catch (e) {
    throw new FetchError({url: url, aborted: true, status: 0, statusText: ''});
  }
};

const getJsonData = async(path: string) => {
  const url = getUrl(path);
  const req = await getFetchResp(url);
  const data = await req.json();
  if (!req.ok) {
    throw new FetchError({url: url, status: req.status, statusText: req.statusText});
  }
  return data;
};

export const getItem = async(path: string) => {
  return await getTextData(path);
};

export const getClipData = async(clipId: ClipId): Promise<Clip> => {
  const data = await getJsonData(`clip/${clipId}`);
  return deserialize('Clip', data);
};

export const getClipsIndexData = async(): Promise<ClipIndexResp[]> => {
  let data: ClipsIndexResp;
  try {
    data = await getJsonData('data/clip-index.json');
  } catch(e) {
    if (e instanceof FetchError) {
      console.warn(`FetchError for "${e.payload.url}": "${e.message}"`);
      return [];
    } else {
      throw e;
    }
  }
  const clipsById: {[k: ClipId]: ClipIndexResp} = Object.fromEntries(data.clips.map((clip) => [clip.id, clip]));
  const clipIds = data.clips.map((clip) => parseInt(clip.id));
  clipIds.sort((a, b) => (a - b));
  clipIds.reverse();
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

function splitPathParts(path: string): string[] {
  return path.split('/').filter((s) => s.length > 0);
}

export function getFileUrl(rootPath: string, path: string, clip?: Clip): URL {
  let pathParts: string[] = splitPathParts(rootPath);
  if (clip) {
    pathParts = [...pathParts, ...splitPathParts(clip.rootDir)];
  }
  pathParts = [...pathParts, ...splitPathParts(path)];
  return getUrl(pathParts.join('/'));
}

export function getUrl(path: string): URL {
  let baseUrl = HOST_PATH;
  if (!baseUrl) throw new Error('CONTENT_HOST_PATH not set');
  if (!baseUrl.endsWith('/')) baseUrl = `${baseUrl}/`;
  const fullPath = `${baseUrl}${path}`;
  return new URL(fullPath);
}
