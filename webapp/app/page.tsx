import ClipList from "./components/ClipList";
import { getClipsIndexData } from "@/app/utils";

export default async function Home() {
  const clipsIndex = await getClipsIndexData();
  return (
    // <main className="flex min-h-screen flex-col items-center justify-between p-24">
    <main>
      <ClipList clips={clipsIndex} />
    </main>
  );
}
