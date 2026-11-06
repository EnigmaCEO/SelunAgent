import { Suspense } from "react";
import ChatApp from "./chat/ChatApp";

export default function Home() {
  return (
    <Suspense fallback={null}>
      <ChatApp />
    </Suspense>
  );
}
