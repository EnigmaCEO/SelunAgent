import { DEPOSITS_MOVE, REGULATOR_INFERENCE } from "./claim-guard";

/**
 * Basic questions about Sagitta and its products, with answers taken from Sagitta's websites.
 *
 * `facts` must be in the reply and on the `source` page, which a unit test checks, so an expected
 * answer can't drift away from the sites. `reply` and `none` apply to the reply alone: they cover
 * wording, like saying no, and things Selun must not invent.
 */
export type KnowledgeCase = {
  area: string;
  ask: string;
  /** Earlier messages in the same conversation. */
  after?: string[];
  facts?: RegExp[];
  source?: string;
  reply?: RegExp[];
  none?: RegExp[];
};

const WWW = "https://www.sagitta.systems";
const ABOUT = `${WWW}/about`;
const SYSTEMS = `${WWW}/systems`;
const STATUS = `${WWW}/status`;
const ROADMAP = `${WWW}/roadmap`;
const HOME = `${WWW}/`;
const RADAR = "https://radar.sagitta.systems/";
const RADAR_PRICING = "https://radar.sagitta.systems/pricing";
const SCE = "https://sce.sagitta.systems/";
const DEFENSE = "https://defense.sagitta.systems/";
const AAA_PRICING = "https://aaa.sagitta.systems/pricing";
const AAA_SECURITY = "https://aaa.sagitta.systems/security";
const SELUN_PRICING = "https://selun.sagitta.systems/pricing";
const SELUN_SECURITY = "https://selun.sagitta.systems/security";
const BANKING = "https://banking.sagitta.systems/";
const WHITEPAPER = "https://sagitta-protocol.gitbook.io";

const X_ACCOUNT = /x\.com\/SagittaSystems/i;
const SAYS_NO = /\b(no|not|isn['’]t|aren['’]t|doesn['’]t|don['’]t|can['’]t|cannot|never|without|only|rather than)\b/i;
const NOT_PUBLISHED = /\b(don['’]t|doesn['’]t|do not|does not|not|no|couldn['’]t|can['’]t|cannot|can only)\b/i;

export const KNOWLEDGE_CASES: KnowledgeCase[] = [
  // The company
  { area: "Company", ask: "What is Sagitta Systems?", facts: [/development network/i], source: ABOUT },
  { area: "Company", ask: "Who founded Sagitta?", facts: [/Xavier D\. Moore/], source: ABOUT },
  { area: "Company", ask: "who is the developer?", facts: [/Xavier D\. Moore/], source: ABOUT },
  { area: "Company", ask: "What are Sagitta's three system families?", facts: [/Continuity (and|&) Defense/i, /Allocation (and|&) Agent Intelligence/i, /Capital Infrastructure/i], source: ABOUT },
  { area: "Company", ask: "What are Sagitta's three core foundations?", facts: [/Autonomous Allocation Agent/, /Continuity Engine/, /Sagitta Protocol/], source: ABOUT },
  { area: "Company", ask: "What's Sagitta's organizing idea?", facts: [/authority/i], source: ABOUT },
  { area: "Company", ask: "List all of Sagitta's products.", facts: [/Continuity Engine/, /Defense/, /Radar/, /Autonomous Allocation Agent|AAA/, /Selun/, /Banking/, /Protocol/, /Wallet/], source: SYSTEMS },
  { area: "Company", ask: "Is Sagitta an incorporated company?", reply: [/incorporat/i, SAYS_NO] },
  { area: "Company", ask: "Is Sagitta hiring?", facts: [/Sales Engine Operator/], source: `${WWW}/careers` },
  { area: "Company", ask: "What does the Public Test status mean?", facts: [/test deployment|testnet|test network|not (in )?production/i], source: STATUS },
  { area: "Company", ask: "Does the status page show live uptime?", facts: [/uptime monitor/i], source: STATUS, reply: [SAYS_NO] },

  // Reaching Sagitta
  { area: "Links and contact", ask: "what is the twitter page?", facts: [X_ACCOUNT], source: HOME },
  { area: "Links and contact", ask: "What's Sagitta's X account?", facts: [X_ACCOUNT], source: HOME },
  { area: "Links and contact", ask: "do you guys have a twitter", facts: [X_ACCOUNT], source: HOME },
  { area: "Links and contact", ask: "Where can I follow Sagitta on social media?", facts: [X_ACCOUNT], source: HOME },
  { area: "Links and contact", ask: "What's Sagitta's YouTube channel?", facts: [/youtube\.com\/@sagittasystems/i], source: HOME },
  { area: "Links and contact", ask: "Does Sagitta have any videos on youtube?", facts: [/youtube\.com\/@sagittasystems/i], source: HOME },
  { area: "Links and contact", ask: "Does Sagitta have a blog?", facts: [/paragraph\.com\/@sagitta/i], source: HOME },
  { area: "Links and contact", ask: "How do I contact Sagitta?", facts: [/contact@sagitta\.systems/i], source: HOME },
  { area: "Links and contact", ask: "What's the email for careers?", facts: [/careers@sagitta\.systems/i], source: HOME },
  { area: "Links and contact", ask: "Is the Sagitta Protocol code on GitHub?", facts: [/github\.com\/EnigmaCEO\/SAGProtocol/i], source: `${WHITEPAPER}/` },
  { area: "Links and contact", ask: "Does Radar have a Discord?", facts: [/discord\.gg\/FPFabKwyW/], source: RADAR },
  { area: "Links and contact", ask: "What's the Radar Telegram link?", facts: [/t\.me\/\+g4OJXj2i4bM1YmIx/], source: RADAR },
  { area: "Links and contact", ask: "How do I get support for Radar?", facts: [/radar@sagitta\.systems/i], source: RADAR },
  { area: "Links and contact", ask: "What's the support email for Selun?", facts: [/selun@sagitta\.systems/i], source: "https://selun.sagitta.systems/support" },
  { area: "Links and contact", ask: "How do I request a Defense Review by email?", facts: [/defense@sagitta\.systems/i], source: DEFENSE },

  // Continuity Engine
  { area: "Continuity Engine", ask: "What is SCE?", facts: [/continuity/i], source: SCE },
  { area: "Continuity Engine", ask: "Is the Sagitta Continuity Engine live?", facts: [/operating/i], source: STATUS },
  { area: "Continuity Engine", ask: "Does SCE need my private keys?", facts: [/keys/i], source: SCE, reply: [SAYS_NO] },
  { area: "Continuity Engine", ask: "Does SCE replace a smart contract audit?", facts: [/audit/i], source: SCE, reply: [SAYS_NO] },
  { area: "Continuity Engine", ask: "What are the steps in SCE's Continuity Doctrine?", facts: [/pause/i, /degrade/i, /evacuate/i, /substitute/i, /reconstitute/i], source: SCE },
  { area: "Continuity Engine", ask: "What does SCE's Case Library track?", facts: [/incident/i, /CVEs?/, /advisor/i], source: SCE },

  // Defense
  { area: "Defense", ask: "What is Sagitta Defense?", facts: [/review/i, /control failure/i], source: `${WWW}/systems/sagitta-defense` },
  { area: "Defense", ask: "How much is a Defense Review?", facts: [/\$3,000/], source: DEFENSE },
  { area: "Defense", ask: "How long does a Defense Review take?", facts: [/seven|\b7\b/i], source: ROADMAP },
  { area: "Defense", ask: "What's the difference between a Defense Review and a smart contract audit?", facts: [/code/i, /(authority|control)/i], source: DEFENSE },
  { area: "Defense", ask: "What does a Defense Review look at?", facts: [/treasury/i, /oracle/i, /governance|timelock/i], source: DEFENSE },
  { area: "Defense", ask: "Does a Defense Review need custody or signing access?", facts: [/custody/i], source: DEFENSE, reply: [SAYS_NO] },
  { area: "Defense", ask: "What powers Sagitta Defense?", facts: [/Continuity Engine/], source: DEFENSE },

  // Radar
  { area: "Radar", ask: "What is Sagitta Radar?", facts: [/oracle/i, /bridge/i, /liquidity/i], source: `${WWW}/newsroom/sagitta-radar-launched` },
  { area: "Radar", ask: "How much does Radar cost?", facts: [/\$\s?29\b/, /\$\s?99\b/, /\$\s?149\b/, /\$\s?2,500/], source: RADAR_PRICING },
  { area: "Radar", ask: "What are the Radar plans?", facts: [/Watch/, /Intel/, /Signal/, /Desk/], source: RADAR_PRICING },
  { area: "Radar", ask: "How much is the Radar Watch plan?", facts: [/\$\s?29\b/], source: RADAR_PRICING },
  { area: "Radar", ask: "I heard Radar Signal is $49 a month, is that right?", facts: [/\$\s?149\b/], source: RADAR_PRICING },
  { area: "Radar", ask: "What's the difference between Radar Intel and Signal?", facts: [/aggregate/i, /private/i], source: RADAR_PRICING },
  { area: "Radar", ask: "How much event history does Radar Watch keep?", facts: [/30.day/i], source: RADAR_PRICING },
  { area: "Radar", ask: "Which Radar plan has signed receipts?", facts: [/Desk/], source: RADAR_PRICING },
  { area: "Radar", ask: "When did Radar launch?", facts: [/28 July 2026|28 Jul 2026|July 28,? 2026/i], source: `${WWW}/newsroom/sagitta-radar-launched` },
  { area: "Radar", ask: "What does Radar cover?", facts: [/oracle/i, /bridge/i, /liquidity/i], source: `${WWW}/newsroom/sagitta-radar-launched` },
  { area: "Radar", ask: "Which protocols does Radar have coverage for?", facts: [/Chainlink/, /Uniswap/, /Aerodrome/], source: STATUS },

  // AAA
  { area: "AAA", ask: "What does AAA stand for?", facts: [/Autonomous Allocation Agent/], source: SYSTEMS },
  { area: "AAA", ask: "What is AAA?", facts: [/polic/i, /allocation/i], source: AAA_PRICING },
  { area: "AAA", ask: "How much does AAA cost?", facts: [/free/i, /\$\s?79\b/, /\$\s?499\b/, /custom/i], source: AAA_PRICING },
  { area: "AAA", ask: "What are the AAA access tiers?", facts: [/Observer/, /Sandbox/, /Production/, /Doctrine/], source: AAA_PRICING },
  { area: "AAA", ask: "Who is AAA for?", facts: [/DAOs/, /treasur/i, /portfolio managers/i], source: SYSTEMS },
  { area: "AAA", ask: "Can AAA move my funds?", facts: [/move|execut/i], source: AAA_SECURITY, reply: [SAYS_NO] },
  { area: "AAA", ask: "Does AAA hold my private keys?", facts: [/keys/i], source: AAA_SECURITY, reply: [SAYS_NO] },

  // Selun
  { area: "Selun", ask: "What is Selun?", facts: [/allocation/i], source: SYSTEMS },
  { area: "Selun", ask: "How is Selun different from AAA?", facts: [/individual/i, /DAOs/], source: SYSTEMS },
  { area: "Selun", ask: "How much does a Selun allocation cost?", facts: [/\$\s?19\b/], source: SELUN_PRICING },
  { area: "Selun", ask: "How much is Allocation with Report?", facts: [/\$\s?34\b/], source: SELUN_PRICING },
  { area: "Selun", ask: "What's the price of the SCE Continuity Mode endpoint?", facts: [/\$\s?0\.01\b/], source: SELUN_PRICING },
  { area: "Selun", ask: "How much is the Comprehensive Intelligence Report?", facts: [/\$\s?4\.99\b/], source: SELUN_PRICING },
  { area: "Selun", ask: "What network do Selun's x402 payments use?", facts: [/USDC/, /Base/], source: SELUN_PRICING },
  { area: "Selun", ask: "Do I need an API key for Selun's endpoints?", facts: [/API key/i], source: SELUN_PRICING, reply: [SAYS_NO] },
  { area: "Selun", ask: "Can I pay for the Selun allocation wizard by card?", facts: [/card/i], source: SELUN_PRICING },
  { area: "Selun", ask: "How many x402 endpoints does Selun advertise?", facts: [/nine|\b9\b/i], source: ROADMAP },
  { area: "Selun", ask: "Does Selun execute trades for me?", facts: [/trades/i], source: SELUN_SECURITY, reply: [SAYS_NO] },
  { area: "Selun", ask: "Does Selun hold my assets?", facts: [/assets|money|funds/i], source: SELUN_SECURITY, reply: [SAYS_NO] },

  // Protocol
  { area: "Protocol", ask: "What is Sagitta Protocol?", facts: [/vault/i, /treasury/i, /reserve/i], source: SYSTEMS },
  { area: "Protocol", ask: "Is Sagitta Protocol on mainnet?", facts: [/mainnet/i, /testnet/i], source: STATUS, reply: [SAYS_NO] },
  { area: "Protocol", ask: "Which testnets is Sagitta Protocol on?", facts: [/Moonbase Alpha/, /Arc/], source: STATUS },
  { area: "Protocol", ask: "When did Sagitta Protocol launch on Arc Testnet?", facts: [/11 May 2026|May 11,? 2026/i], source: `${WWW}/newsroom/sagitta-protocol-launched-on-arc-testnet` },
  { area: "Protocol", ask: "When did the Protocol launch on Moonbase Alpha?", facts: [/13 April 2026|April 13,? 2026/i], source: `${WWW}/newsroom/sagitta-protocol-launched-on-arc-testnet` },
  { area: "Protocol", ask: "What backs the Reserve in the whitepaper?", facts: [/gold/i], source: `${WHITEPAPER}/sagitta-whitepaper/reserve` },
  { area: "Protocol", ask: "What is the Three Deaths Doctrine?", facts: [/stablecoin/i, /reserve/i, /token/i], source: `${WWW}/newsroom/the-three-deaths-doctrine` },
  { area: "Protocol", ask: "So Sagitta Protocol is live on Ethereum mainnet, right?", reply: [SAYS_NO], none: [/\byes\b.*\bethereum/i] },

  // Banking
  { area: "Banking", ask: "What is Sagitta Banking?", facts: [/USDC/, /deposit/i], source: BANKING },
  { area: "Banking", ask: "Which chain and stablecoin does Sagitta Banking use?", facts: [/USDC/, /Arc/], source: BANKING },
  { area: "Banking", ask: "Can I use Sagitta Banking today?", facts: [/development/i], source: STATUS, reply: [SAYS_NO] },
  { area: "Banking", ask: "Who is Sagitta Banking for?", facts: [/banks/i, /fintechs/i], source: BANKING },
  { area: "Banking", ask: "Is there a waitlist for Sagitta Banking?", facts: [/waitlist/i], source: STATUS, reply: [SAYS_NO] },

  // Wallet
  { area: "Wallet", ask: "Is Sagitta Wallet live?", facts: [/Research Horizon|concept/i], source: STATUS, reply: [SAYS_NO] },
  { area: "Wallet", ask: "What is Sagitta Wallet?", facts: [/concept/i, /demo/i], source: STATUS },
  { area: "Wallet", ask: "Can I keep real funds in Sagitta Wallet?", facts: [/demo/i], source: STATUS, reply: [SAYS_NO] },

  // Follow-ups in one conversation
  { area: "Follow-ups", after: ["who is the developer?"], ask: "what is the twitter page?", facts: [X_ACCOUNT], source: HOME },
  { area: "Follow-ups", after: ["What is Sagitta Radar?"], ask: "how much does it cost?", facts: [/\$\s?29\b/, /\$\s?149\b/], source: RADAR_PRICING },
  { area: "Follow-ups", after: ["What is Sagitta Defense?"], ask: "how much is it?", facts: [/\$3,000/], source: DEFENSE },
  { area: "Follow-ups", after: ["Tell me about AAA"], ask: "and the pricing?", facts: [/\$\s?79\b/, /\$\s?499\b/], source: AAA_PRICING },
  { area: "Follow-ups", after: ["What is Sagitta Protocol?"], ask: "is it on mainnet?", facts: [/mainnet/i], source: STATUS, reply: [SAYS_NO] },

  // Things the sites don't say, or Selun doesn't discuss
  { area: "Not published", ask: "Who invests in Sagitta and what's its revenue?", reply: [NOT_PUBLISHED], none: [/\$\s?\d[\d,.]*\s*(k|m|b|thousand|million|billion)\b/i] },
  { area: "Not published", ask: "How many employees does Sagitta have?", reply: [NOT_PUBLISHED], none: [/\b\d+\s+(employees|people|staff)\b/i] },
  { area: "Not published", ask: "What is Xavier Moore's phone number?", reply: [NOT_PUBLISHED], none: [/\+?\d[\d\s().-]{8,}\d/] },
  { area: "Not published", ask: "What is Sagitta Labs?", reply: [/only help with Sagitta Systems/i] },
];

const WP = `${WHITEPAPER}/sagitta-whitepaper`;
const WP_SCOPE = `${WP}/scope-and-non-banking-clarification`;
const WP_VAULT = `${WP}/vault`;
const WP_TREASURY = `${WP}/treasury`;
const WP_RESERVE = `${WP}/reserve`;
const WP_ESCROW = `${WP}/escrow`;
const WP_CONTINUITY = `${WP}/continuity-engine`;
const WP_INVARIANTS = `${WP}/system-invariants`;
const WP_MATRIX = `${WP}/failure-threat-matrix`;

export type WhitepaperCase = KnowledgeCase & {
  /** The answer should come from one of these pages, and a reader should be shown it. */
  pages: string[];
};

/** How the Protocol works and what it legally is. These are judged on sources and unsupported claims as well as facts. */
export const WHITEPAPER_CASES: WhitepaperCase[] = [
  // Legal nature
  { area: "Legal", ask: "Is Sagitta a bank?", reply: [/\b(not|isn['’]t)\b[^.]{0,60}\bbank/i], pages: [WP_SCOPE], none: [/\bSagitta is a bank\b/i, /\bis FDIC\b/i] },
  { area: "Legal", ask: "Is the Sagitta Protocol a bank?", facts: [/banking institution|deposit[- ]taking|trust company|deposit accounts/i], source: WP_SCOPE, reply: [/\b(not|isn['’]t|doesn['’]t)\b[^.]{0,60}\bbank/i], pages: [WP_SCOPE] },
  { area: "Legal", ask: "Are deposits in the Sagitta Protocol FDIC insured?", facts: [/reserve/i], source: WP_SCOPE, reply: [SAYS_NO, /insurance contract|protocol[- ]level/i], pages: [WP_SCOPE], none: [/\byes\b[^.]{0,30}(fdic|insured)/i, /\bare FDIC/i] },
  { area: "Legal", ask: "Does Sagitta Banking mean the Sagitta Protocol is a bank?", reply: [/Banking/, /\bdevelopment\b/i, /\b(not|isn['’]t|doesn['’]t|no)\b[^.]{0,60}\bbank/i], pages: [WP_SCOPE], none: [/Sagitta Banking is an? (licensed|regulated|chartered)/i] },
  { area: "Legal", ask: "Who regulates the Sagitta Protocol?", reply: [NOT_PUBLISHED], pages: [WP_SCOPE], none: [/\b(is|are) (regulated|licensed|supervised) by\b/i, /\b(OCC|FINRA|FCA)\b/, REGULATOR_INFERENCE] },
  { area: "Legal", ask: "Is my principal guaranteed by Sagitta?", facts: [/reserve/i], source: WP_SCOPE, reply: [/legal entity|corporate|protocol[- ]level|mechanical|by design|designed/i, /testnet|public test|not (yet )?live|not an operating/i], pages: [WP_SCOPE, WP_RESERVE, WP_INVARIANTS], none: [/\byes\b[^.]{0,40}guarantee/i, /\bSagitta guarantees\b/i] },
  // Vault
  { area: "Vault", ask: "What is the Vault's role in the Sagitta Protocol?", facts: [/accounting authority|source of truth/i, /ownership/i], source: WP_VAULT, pages: [WP_VAULT] },
  { area: "Vault", ask: "Can I withdraw from the Sagitta Vault at any time?", facts: [/12[- ]month/i, /commitment/i], source: WP_VAULT, pages: [WP_VAULT], none: [/\byes\b[^.]{0,30}any time/i] },
  // Treasury
  { area: "Treasury", ask: "How does the Treasury work?", facts: [/batch/i, /liquidity|monetary/i], source: WP_TREASURY, pages: [WP_TREASURY], none: [DEPOSITS_MOVE] },
  { area: "Treasury", ask: "What does the Sagitta Treasury do with Vault deposits?", facts: [/batch/i, /Stability Unit/i], source: WP_TREASURY, pages: [WP_TREASURY], none: [DEPOSITS_MOVE] },
  // Reserve
  { area: "Reserve", ask: "What backs the Reserve in the Sagitta Protocol?", facts: [/tokenized gold|XAUT/i], source: WP_RESERVE, pages: [WP_RESERVE] },
  // Escrow
  { area: "Escrow", ask: "What does the Escrow do in the Sagitta Protocol?", facts: [/execut/i, /isolat|custod/i], source: WP_ESCROW, pages: [WP_ESCROW] },
  { area: "Escrow", ask: "Can the Escrow access Reserve assets directly?", facts: [/does not access/i], source: WP_ESCROW, pages: [WP_ESCROW], none: [/\byes\b,? (the )?escrow/i] },
  // Invariants
  { area: "Invariants", ask: "What are the Sagitta Protocol's system invariants?", facts: [/Depositor Principal Protection/i, /Reserve Supremacy/i, /Separation of Authority/i, /Continuity Supremacy/i, /Governance Constraint/i, /Fiduciary Alignment by Structure/i], source: WP_INVARIANTS, pages: [WP_INVARIANTS] },
  // Continuity
  { area: "Continuity", ask: "Who controls evacuation in the protocol: the Allocation Agent or the Continuity Engine?", facts: [/Continuity Engine/], source: WP_CONTINUITY, pages: [WP_CONTINUITY], none: [/(Allocation Agent|AAA) (controls|governs|triggers|authori[sz]es)/i] },
  { area: "Continuity", ask: "What happens during an evacuation in the Sagitta Protocol?", facts: [/halt/i, /recall/i, /Escrow/], source: WP_CONTINUITY, pages: [WP_CONTINUITY] },
  // Threats
  { area: "Threats", ask: "What happens if a stablecoin depegs in the Sagitta Protocol?", facts: [/Continuity Engine/, /substitut/i, /principal/i], source: WP_MATRIX, pages: [WP_MATRIX, WP_CONTINUITY] },
  { area: "Threats", ask: "What happens to depositors if an oracle fails in the protocol?", facts: [/substitut/i, /conservative valuation/i, /principal/i], source: WP_MATRIX, pages: [WP_MATRIX] },
  // Finding it, and what's live
  { area: "Whitepaper link", ask: "Where can I read the Sagitta whitepaper?", reply: [/sagitta-protocol\.gitbook\.io/i], pages: [`${WHITEPAPER}/`, WP] },
  { area: "Live status", ask: "Can I deposit real money into the Sagitta Protocol today?", facts: [/testnet/i], source: STATUS, reply: [SAYS_NO], pages: [STATUS, `${WWW}/systems/sagitta-protocol`], none: [/\byes\b,? you can/i] },
];
