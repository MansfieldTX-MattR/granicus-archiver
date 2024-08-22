import { root } from "postcss";
import type { ClipIndexResp } from "./utils";

export type ClipId = string;
export type PathLike = string;

interface HasTypeName {
  __typeName: string;
}

export interface ParseClipData extends HasTypeName {
  id: ClipId;
  location: string;
  name: string;
  duration: number;
  datetime: Date;
}

export interface ParseClipDataResp extends Omit<ParseClipData, '__typeName' | 'datetime'> {
  date: number;
}


export interface FileMeta extends HasTypeName {
  contentLength: number;
  contentType: string;
  lastModified?: Date;
  etag?: string;
}

export interface FileMetaResp {
  content_length: number;
  content_type: string;
  last_modified?: string;
  etag?: string;
}

interface BaseClipFiles<T> {
  agenda?: T;
  minutes?: T;
  audio?: T;
  video?: T;
  chapters?: T;
}

export type ClipFileKey = keyof BaseClipFiles<any>;

type ClipFileMeta = BaseClipFiles<FileMeta>;
type ClipFileMetaResp = BaseClipFiles<FileMetaResp>;

export type ClipFiles = {
  metadata: ClipFileMeta,
} & BaseClipFiles<PathLike> & HasTypeName;

type ClipFilesResp = {
  metadata: ClipFileMetaResp,
} & BaseClipFiles<PathLike>;


interface ClipBase<FilesT, DataT> {
  id: ClipId;
  rootDir: PathLike;
  files: FilesT;
  data: DataT;
}

export type Clip = {
  location: string,
} & ClipBase<ClipFiles, ParseClipData> & HasTypeName;
export type ClipResp = {
  root_dir: PathLike,
  parse_data: ParseClipDataResp,
} & Omit<ClipBase<ClipFilesResp, ParseClipDataResp>, 'rootDir' | 'data'>;


type ClipDict = Map<ClipId, Clip>;
type ClipDictResp = Map<ClipId, ClipResp>;

interface ClipCollectionBase<ClipT> {
  clips: ClipT;
}

export type ClipCollection = {
  baseDir: PathLike,
  clipsByCategory: Map<string, Set<ClipId>>,
} & ClipCollectionBase<ClipDict> & HasTypeName;

export type ClipCollectionResp = {
  base_dir: PathLike,
} & ClipCollectionBase<ClipDictResp>;


type ModelMapT = {
  'ParseClipData': ParseClipData,
  'FileMeta': FileMeta,
  'ClipFileMeta': ClipFileMeta,
  'ClipFiles': ClipFiles,
  'Clip': Clip,
  'ClipCollection': ClipCollection,
}

type ModelRespT = {
  'ParseClipData': ParseClipDataResp,
  'FileMeta': FileMetaResp,
  'ClipFileMeta': ClipFileMetaResp,
  'ClipFiles': ClipFilesResp,
  'Clip': ClipResp,
  'ClipCollection': ClipCollectionResp,
}


type ModelType<Tn extends keyof ModelMapT> = ModelMapT[Tn];
type ModelResp<Tn extends keyof ModelRespT> = ModelRespT[Tn];





function castResponseType<Tn extends keyof ModelRespT>(typeName: Tn, data: Object): ModelResp<Tn> {
  return data as ModelResp<Tn>;
}

export function deserialize<Tn extends keyof ModelMapT>(typeName: Tn, srcData: ModelResp<Tn>): ModelType<Tn> {
  let result: ModelMapT[Tn];
  if (typeName === 'ParseClipData') {
    const data = castResponseType('ParseClipData', srcData);
    result = {
      id: data.id,
      location: data.location,
      name: data.name,
      duration: data.duration,
      datetime: new Date(data.date * 1000),
      __typeName: typeName,
    } as ModelMapT[Tn];
  } else if (typeName === 'FileMeta') {
    const data = castResponseType('FileMeta', srcData);
    result = {
      contentLength: data.content_length,
      contentType: data.content_type,
      lastModified: data.last_modified ? new Date(Date.parse(data.last_modified)) : undefined,
      etag: data.etag,
      __typeName: typeName,
    } as ModelMapT[Tn];
  } else if (typeName === 'ClipFileMeta') {
    const data = castResponseType('ClipFileMeta', srcData);
    result = {
      agenda: data.agenda ? deserialize('FileMeta', data.agenda) : undefined,
      minutes: data.minutes? deserialize('FileMeta', data.minutes) : undefined,
      audio: data.audio ? deserialize('FileMeta', data.audio) : undefined,
      video: data.video ? deserialize('FileMeta', data.video) : undefined,
    } as ModelMapT[Tn];
  } else if (typeName === 'ClipFiles') {
    const data = castResponseType('ClipFiles', srcData);
    const meta = deserialize('ClipFileMeta', data.metadata);
    result = {
      agenda: data.agenda,
      minutes: data.minutes,
      audio: data.audio,
      video: data.video,
      chapters: data.chapters,
      metadata: meta,
      __typeName: typeName,
    } as ModelMapT[Tn];
  } else if (typeName === 'Clip') {
    const data = castResponseType('Clip', srcData);
    const clipFiles = deserialize('ClipFiles', data.files);
    const parseData = deserialize('ParseClipData', data.parse_data);
    result = {
      id: parseData.id,
      location: parseData.location,
      rootDir: data.root_dir,
      files: clipFiles,
      data: parseData,
      __typeName: typeName,
    } as ModelMapT[Tn];
  } else if (typeName === 'ClipCollection') {
    const data = castResponseType('ClipCollection', srcData);
    const clips: ClipDict = new Map();
    const byCategory = buildClipsCategories(Object.values(data.clips));
    result = {
      baseDir: data.base_dir,
      clips: clips,
      clipsByCategory: byCategory,
      __typeName: typeName,
    } as ModelMapT[Tn];
  } else {
    throw new Error(`Invalid type name "${typeName}"`);
  }
  return result;
}


export const buildClipsCategories = (clips: (Clip|ClipIndexResp)[]): Map<string, Set<ClipId>> => {
  const byCategory: Map<string, Set<ClipId>> = new Map();
  clips.forEach((clip) => {
    let catSet = byCategory.get(clip.location);
      if (!catSet) {
        catSet = new Set();
        byCategory.set(clip.location, catSet);
      }
      catSet.add(clip.id);
  });
  return byCategory;
};

// export const loadData = (jsonData: string): ClipCollection => {
//   const data = JSON.parse(jsonData);
//   return deserialize('ClipCollection', data);
// };
