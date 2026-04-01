import EarnPageClient from "./page-client";

type EarnPageProps = {
  searchParams: Promise<{
    wallet?: string | string[];
  }>;
};

export default async function EarnPage(props: EarnPageProps) {
  const searchParams = await props.searchParams;
  const walletParam = Array.isArray(searchParams.wallet) ? searchParams.wallet[0] : searchParams.wallet;

  return <EarnPageClient initialWalletQuery={walletParam ?? null} />;
}
