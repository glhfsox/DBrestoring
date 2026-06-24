import { z } from "zod";

export const contactSchema = z.object({
  name: z.string().trim().min(1, "Name is required").max(100),
  email: z.string().trim().email("Enter a valid email").max(200),
  company: z.string().trim().max(120).optional().default(""),
  teamSize: z.string().trim().max(40).optional().default(""),
  plan: z.enum(["team", "enterprise", "other"]).optional().default("other"),
  message: z.string().trim().min(1, "Tell us a little about your needs").max(4000),
  // honeypot: must stay empty
  website: z.string().max(0).optional().default(""),
  turnstileToken: z.string().optional().default(""),
});

export type ContactInput = z.infer<typeof contactSchema>;
