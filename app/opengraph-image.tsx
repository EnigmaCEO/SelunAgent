import { renderSocialCard } from "./lib/social-card";

export const runtime = "edge";
export const alt = "Selun AI: onchain intelligence for people and agents";
export const size = {
  width: 1200,
  height: 630,
};
export const contentType = "image/png";

export default function OpenGraphImage() {
  return renderSocialCard();
}
