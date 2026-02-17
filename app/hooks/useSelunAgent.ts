"use client";

import { useCallback, useState } from "react";
import type { AgentRequest, AgentResponse, WizardContext } from "@/app/types/agent";

export type SelunMessage = {
  id: string;
  role: "user" | "agent";
  content: string;
};

const buildId = () => {
  if (typeof crypto !== "undefined" && crypto.randomUUID) return crypto.randomUUID();
  return `selun-${Date.now()}-${Math.random().toString(16).slice(2)}`;
};

const toAgentHistory = (messages: SelunMessage[]): AgentRequest["history"] =>
  messages.map((message) => ({
    role: message.role === "agent" ? "assistant" : "user",
    content: message.content,
  }));

async function messageSelun(payload: AgentRequest): Promise<AgentResponse> {
  const response = await fetch("/api/agent", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });

  const data = (await response.json()) as AgentResponse;
  if (!response.ok) {
    return {
      success: false,
      error: data.error ?? "Selun could not process that request.",
    };
  }

  return data;
}

export function useSelunAgent() {
  const [messages, setMessages] = useState<SelunMessage[]>([]);
  const [isThinking, setIsThinking] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const sendMessage = useCallback(
    async (userMessage: string, context?: WizardContext) => {
      const trimmedMessage = userMessage.trim();
      if (!trimmedMessage || isThinking) return;

      setError(null);

      const userEntry: SelunMessage = {
        id: buildId(),
        role: "user",
        content: trimmedMessage,
      };
      const currentMessages = [...messages, userEntry];
      setMessages(currentMessages);
      setIsThinking(true);

      try {
        const result = await messageSelun({
          userMessage: trimmedMessage,
          context,
          history: toAgentHistory(currentMessages),
        });
        const responseText = result.response;

        if (!result.success || !responseText) {
          const errorMessage = result.error ?? "Selun could not generate a response.";
          setError(errorMessage);
          setMessages((prev) => [
            ...prev,
            {
              id: buildId(),
              role: "agent",
              content: errorMessage,
            },
          ]);
          return;
        }

        setMessages((prev) => [
          ...prev,
          {
            id: buildId(),
            role: "agent",
            content: responseText,
          },
        ]);
      } catch (caughtError) {
        const errorMessage = caughtError instanceof Error ? caughtError.message : "Unexpected Selun API error.";
        setError(errorMessage);
        setMessages((prev) => [
          ...prev,
          {
            id: buildId(),
            role: "agent",
            content: errorMessage,
          },
        ]);
      } finally {
        setIsThinking(false);
      }
    },
    [isThinking, messages],
  );

  return {
    messages,
    sendMessage,
    isThinking,
    error,
  };
}
