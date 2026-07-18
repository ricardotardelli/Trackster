// Save as test.mjs and run: node test.mjs

import OpenAI from "openai";

const client = new OpenAI();

const response = await client.chat.completions.create({
  model: "openai.gpt-oss-120b",
  messages: [{ role: "user", content: "What is Amazon Bedrock?" }],
});
console.log(response.choices[0].message.content);