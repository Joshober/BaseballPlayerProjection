import { useParams } from "react-router-dom";
import PlayerCard from "../components/PlayerCard";

export default function PlayerPage() {
  const { mlbamId } = useParams();
  const id = Number(mlbamId);
  if (!Number.isFinite(id)) {
    return <p className="p-6 text-scout-chalk">Invalid player id</p>;
  }
  return (
    <div className="min-h-screen bg-scout-ink p-4 md:p-8">
      <PlayerCard mlbamId={id} />
    </div>
  );
}
