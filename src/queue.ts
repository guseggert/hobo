export interface WorkMessage<T = any> {
  id: string;
  body: T;
}

export interface WorkQueue<T = any> {
  send(signal: AbortSignal, msg: T): Promise<void>;
  receive(
    signal: AbortSignal,
    maxMessages?: number,
    waitSeconds?: number
  ): Promise<WorkMessage<T>[]>;
  delete(signal: AbortSignal, id: string, receipt?: string): Promise<void>;
}
