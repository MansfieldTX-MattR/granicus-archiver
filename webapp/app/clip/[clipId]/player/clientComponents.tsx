'use client';
import { useState, useEffect, useTransition, useRef, useId } from "react";

import type { Clip } from "@/app/model";
import { ToggleButton } from "@/app/components/Button";
import { formatDurationDelta } from "@/app/components/utils";
import { mergeClassNames } from "@/app/components/utils";


interface VideoProps {
  clip: Clip;
  videoUrl?: string;
  chaptersUrl?: string;
  className?: string;
  col1Class?: string;
  col2Class?: string;
  videoClass?: string;
}

export const Video = (
  {clip, videoUrl, chaptersUrl, className, col1Class, col2Class, videoClass}:
  VideoProps
) => {
  const videoRef = useRef<HTMLVideoElement|null>(null);
  const textTrackRef = useRef<TextTrack|null>(null);
  const textTrackId = useId();
  const [ cueList, setCueList ] = useState<VTTCue[]|null>(null);
  const [ activeCues, setActiveCues ] = useState<VTTCue[]|null>(null);

  useEffect(() => {
    if (textTrackRef.current !== null) return;
    const checkTracks = (elem: HTMLMediaElement): boolean => {
      const textTrack = elem.textTracks.getTrackById(textTrackId);
      // console.log('textTrack: ', textTrack);
      if (textTrack !== null) {
        console.log('cue length: ', textTrack.cues?.length);
        if (!(textTrack.cues?.length)) return false;
        const cues = textTrack.cues;
        if (cues !== null) {
          const cuesArr = cueListToArray(cues);
          if (cuesArr.length) setCueList(cuesArr);
        }
        console.log('set textTrackRef');
        textTrackRef.current = textTrack;

        return true;
      }
      return false;
    }
    const vidEl = videoRef.current;
    if (vidEl === null) {
      return;
    }
    if (checkTracks(vidEl)) return;
    let timerId: NodeJS.Timeout|null = null;
    function removeTimer() {
      if (timerId !== null) {
        console.log('removeTimer');
        clearInterval(timerId);
        timerId = null;
      }
    }
    const readyStateHandler = () => {
      console.log('readyState: ', vidEl.readyState);
      if (vidEl.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA) {
        if (textTrackRef.current) {
          removeTimer();
        } else if (checkTracks(vidEl)) {
          removeTimer();
        }
      }
    }
    console.log('setInterval');
    timerId = setInterval(() => {
      readyStateHandler();
      console.log('readyState: ', vidEl.readyState);
    }, 250);
    return () => {
      removeTimer();
    }
  }, [videoRef, textTrackRef, setCueList, textTrackId]);

  function cueListToArray(cueList: TextTrackCueList): VTTCue[] {
    const cuesArr: VTTCue[] = [];
    for (let i=0; i<cueList.length; i++) {
      const cue = cueList[i] as VTTCue;
      cuesArr.push(cue);
    }
    return cuesArr;
  }

  // Bind to textTrack.cuechange
  useEffect(() => {
    const textTrack = textTrackRef.current;
    if (!textTrack) return;

    const handleCueChange = () => {
      console.log('handleCueChange: ', textTrack.activeCues);
      if (textTrack.activeCues !== null) {
        setActiveCues(cueListToArray(textTrack.activeCues));
      }
    };

    textTrack.addEventListener('cuechange', handleCueChange);
    return () => {
      textTrack.removeEventListener('cuechange', handleCueChange);
    }
  }, [textTrackRef.current, setActiveCues]);


  return (
    <div className={mergeClassNames("flex flex-row", className)}>
      <div className={mergeClassNames("flex flex-col", col1Class)}>
        {/* <video ref={videoRef} controls src={videoUrl ? videoUrl.toString() : undefined}> */}
        <video ref={videoRef} controls className={mergeClassNames("px-2 py-2 aspect-video", videoClass)} crossOrigin="use-credentials">
          {/* {videoPath ? <source src={videoPath} type="video/mp4" /> : <></>} */}
          {videoUrl ? <source src={videoUrl} type="video/mp4" /> : <></>}
          {/* {chaptersUrl ? <track src={chaptersUrl.toString()} kind="captions" default srcLang="en" /> : <></>} */}
          {/* <track id={textTrackId} src={`/clip/${clip.id}/webvtt`} default srcLang="en" /> */}
          {chaptersUrl ? <track id={textTrackId} src={chaptersUrl} default kind="chapters" srcLang="en" /> : <></>}
          {/* <track id={textTrackId} src={chaptersUrl.toString()} default kind="chapters" srcLang="en" /> */}
        </video>
      </div>
      <div className={mergeClassNames("flex flex-col", col2Class)}>
        <ul className="list-inside px-1 overflow-y-auto max-h-[80vh]">
          {
            cueList ? cueList.map(
              (cue) =>
                <VideoCue
                  key={cue.id}
                  textCue={cue}
                  activeCues={activeCues}
                  onClick={() => {
                    if (videoRef.current) {
                      videoRef.current.currentTime = cue.startTime;
                    }
                  }}
                />
            ) : undefined
          }
        </ul>
      </div>
    </div>
  );
};

const VideoCue = (
  {textCue, activeCues, onClick}:
  {textCue: VTTCue, activeCues: VTTCue[]|null, onClick: React.MouseEventHandler<HTMLButtonElement>}
) => {
  const [ activeRef, setActiveRef ] = useState<boolean>(false);
  const [isPending, startTransition] = useTransition();
  const itemRef = useRef<HTMLLIElement|null>(null);

  useEffect(() => {
    const item = itemRef.current;
    const isActive = activeCues ? activeCues.filter((c) => c.id === textCue.id).length > 0 : false;

    if (isActive && !activeRef) {
      startTransition(() => {
        setActiveRef(isActive);
        if (item !== null) {
          item.scrollIntoView({
            behavior: 'smooth',
            block: 'start',
          });
        }
      });
    } else if (!isActive && activeRef) {
      setActiveRef(isActive);
    }
  }, [itemRef.current, textCue.id, activeRef, setActiveRef, activeCues]);

  return (
    <li className="w-auto" ref={itemRef}>
      <ToggleButton
        className="my-1 py-4 w-10/12 text-left"
        selected={activeRef}
        onClick={onClick}
      >
        <span className="flex flex-row">
          <span>{textCue.text}</span>
          <span className="ml-auto">{formatDurationDelta(textCue.startTime)}</span>
        </span>
      </ToggleButton>
      {/* <p className="text-white">{formatDurationDelta(textCue.startTime)}</p> */}
    </li>
  );
};
