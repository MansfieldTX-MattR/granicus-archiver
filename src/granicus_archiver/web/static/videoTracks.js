window.addEventListener('load', (loadEvt) => {
  const vidTag = document.getElementById('videoPlayer');
  let textTrack;
  // const textTrack = document.getElementById('videoTextTrack');
  const chapterListEl = document.getElementById('videoChaptersList');
  if (!vidTag) {
    return;
  }
  const hasChapters = vidTag.dataset.hasChapters === 'true';
  // console.log(vidTag.dataset.hasChapters)

  if (!hasChapters) {
    return;
  }

  const cueListToArray = (cueList) => {
    const cuesArr = [];
    for (let i=0; i<cueList.length; i++) {
      const cue = cueList[i];
      cuesArr.push(cue);
    }
    return cuesArr;
  };

  const getTrackItems = () => {
    if (vidTag.readyState < HTMLMediaElement.HAVE_CURRENT_DATA) {
      return null;
    }
    let tt = null;
    if (!textTrack) {
      tt = vidTag.textTracks.getTrackById('videoTextTrack');
      if (tt) {
        textTrack = tt;
      } else {
        return null;
      }
    }
    const cues = textTrack.cues;
    if (!cues || !(cues.length)) {
      return null;
    }
    const cuesArr = cueListToArray(cues);
    if (cuesArr.length) {
      return cuesArr;
    }
    return null;
  };

  let cuesArr = null;

  const checkTracks = () => {
    const items = getTrackItems();
    if (items !== null) {
      cuesArr = items;
      return true;
    }
    return false;
  };

  const buildChapterItems = () => {
    console.assert(cuesArr !== null);
    const itemTemplate = document.querySelector("#videoChaptersListItemTemplate > .list-group-item");
    console.assert(itemTemplate !== undefined);
    const listItems = cuesArr.map(
      (cue) => {
        const item = itemTemplate.cloneNode(true);
        // console.log(cue, item);
        function getIsActive() {
          const activeCues = cueListToArray(textTrack.activeCues);
          if (!activeCues) return false;
          return activeCues.filter((c) => c.id === cue.id).length > 0;
        }
        if (getIsActive()) {
          item.classList.add('active');
        } else {
          item.classList.remove('active');
        }
        item.dataset.cueId = cue.id;
        item.querySelector(".chapter-item-text").textContent = cue.text;
        item.querySelector(".chapter-item-time").textContent = formatDurationDelta(cue.startTime);
        chapterListEl.appendChild(item);
        item.addEventListener('click', (e) => {
          vidTag.currentTime = cue.startTime;
        });
        textTrack.addEventListener('cuechange', () => {
          if (getIsActive()) {
            if (!item.classList.contains('active')) {
              // item.scrollIntoViewIfNeeded();
              if (!item.isScrolledIntoView()) {
                item.scrollIntoView({
                  behavior: 'smooth',
                  block: 'start',
                });
              }
            }
            item.classList.add('active');
          } else {
            item.classList.remove('active');
          }
        });
        return item;
      }
    );
    // const itemsById = Object.fromEntries(listItems.map((item) => [item.dataset.id, item]));
  };

  let timerId = null;

  const removeTimer = () => {
    if (timerId !== null) {
      clearInterval(timerId);
      timerId = null;
    }
  }

  const waitForVidState = () => {
    // console.log(vidTag.readyState);
    if (vidTag.readyState >= HTMLMediaElement.HAVE_CURRENT_DATA) {
      try {
        if (cuesArr !== null) {
          removeTimer();
        } else if (checkTracks()) {
          removeTimer();
          buildChapterItems();
        }
      } catch (err) {
        removeTimer();
        throw(err);
      }
    }
  };

  timerId = setInterval(() => {
    waitForVidState();
  }, 250);
});
