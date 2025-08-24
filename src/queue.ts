export interface WorkMessage<T = any> {
  id: string;
  body: T;
}

export interface WorkQueue<T = any> {
  send(msg: T): Promise<void>;
  receive(
    maxMessages?: number,
    waitSeconds?: number
  ): Promise<WorkMessage<T>[]>;
  delete(id: string, receipt?: string): Promise<void>;
}
