/// <reference types="vite/client" />

/** Injected from repo root `config.yaml` via `client/vite.config.ts` */
declare const __MATCENGINE_CONFIG__: {
  market: {
    name: string;
    stock_prec: number;
    money_prec: number;
    fee_prec: number;
    min_amount?: string;
  };
  brokers?: string;
  db?: { addr?: string; user?: string; passwd?: string };
};
