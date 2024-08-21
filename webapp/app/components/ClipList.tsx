'use client'
import { useState } from "react";
import Link from "next/link";

import type { ClipIndexResp } from "../utils";
import { Column, Columns } from "./Columns";
import { ToggleButton } from "./Button";


export default function ClipList(
  // {clips}: {clips: ClipCollection}
  {clips}: {clips: ClipIndexResp[]}
) {
  // const clipsByCategory: Map<string, Set<ClipId>> = new Map();
  // const clipsByCategory = buildClipsCategories(clips);
  // const clipsById: Map<ClipId, ClipIndexResp> = new Map(clips.map((clip) => [clip.id, clip]));

  // const allCategories = [...clips.clipsByCategory.keys()];
  // const allCategories = clips.clipsByCategory.keys().
  // const allCategories = [...Object.keys(clips.clipsByCategory)];
  // const allCategories = Array.from(clipsByCategory.keys());
  const allCategories = Array.from(new Set(clips.map((clip) => clip.location)));
  // const [ categories, setCategories ] = useState<Set<string>|null>(new Set(allCategories));
  const [ hiddenCategories, setHiddenCategories ] = useState<Set<string>>(new Set());


  const hideCategory = (category: string) => {
    const newHCat = new Set(hiddenCategories);
    newHCat.add(category);
    setHiddenCategories(newHCat);
  };

  const showCategory = (category: string) => {
    const newHCat = new Set(hiddenCategories);
    newHCat.delete(category);
    setHiddenCategories(newHCat);
  };

  const toggleCategory = (category: string) => {
    if (hiddenCategories.has(category)) {
      showCategory(category);
    } else {
      hideCategory(category);
    }
  }

  return (
    <Columns flex={{alignItems: 'center', justifyContent: 'space-between'}}>
      <Column className="w-auto prose dark:prose-invert">
        <ul className="list-inside">
          {/* {displayClips.map((clip) => <ClipListItem key={clip.id} clip={clip} />)} */}
          { clips.filter((clip) => !(hiddenCategories.has(clip.location))).map((clip) =>
            <ClipListItem key={clip.id} clip={clip} />
          )}
        </ul>
      </Column>
      <Column className="w-1/3">
        <p>Categories</p>
        <ul className="list-inside">
          {allCategories.map((category) => {
            return (
              <CategoryListItem
                key={category}
                category={category}
                selected={!(hiddenCategories.has(category))}
                onClick={() => {
                  toggleCategory(category);
                }}
              />
            );
          })}
        </ul>
      </Column>
    </Columns>
  );
}

const ClipListItem = ({clip}: {clip: ClipIndexResp}) => {
  const dt = new Date(Date.parse(clip.datetime));
  return (
    <li><Link href={`/clip/${clip.id}`}>{clip.name} - {dt.toLocaleDateString()}</Link></li>
  );
};

const CategoryListItem = (
  {category, selected, onClick}:
  {category: string, selected: boolean, onClick: React.MouseEventHandler<HTMLButtonElement>}
) => {
  return (
    <li>
      <ToggleButton className="my-2 rounded-md w-full" selected={selected} onClick={onClick}>{category}</ToggleButton>
    </li>
  );
};
