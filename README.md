# Telegram Anonymous Q&A Bot (Classroom Edition)

This is a Telegram Anonymous Q&A Bot designed specifically for classroom use. It provides a safe and moderated space for students to ask questions, with powerful administration tools for the teacher. 

*Credit: Core logic and architecture developed with assistance from Claude.*

## ✨ Features

* **Flexible Submissions:** Students can ask questions anonymously or publicly. The bot accepts text, photos, videos, and voice messages.
* **Upvote System:** Students can anonymously upvote posted questions directly in the group to show shared interest.
* **Smart Moderation:** Utilizes an AI filter powered by Groq (Llama-3.3-70b) and Serper to analyze questions and check URL reputation. High-risk messages (spam or phishing) are not auto-deleted; instead, they are sent to a pending review queue for the teacher.
* **Clarification Requests:** The teacher can request more context from an anonymous student. The bot sends the student an anonymous DM, and their reply is relayed directly back to the teacher.
* **Persistent Data:** Question counts, upvotes, and pending reviews are saved via Upstash Redis, ensuring no data is lost if the bot restarts.
* **Hosting Ready:** Includes a Flask health server to keep the application running on Render's free tier.

---

## 🤖 Commands List

### 🎓 For Students
* `/start` — Initiates the prompt to ask a new question.
* `/cancel` — Cancels the current question draft.
* `/help` — Displays a user-facing guide on how to use the bot.

### 🧑‍🏫 For the Teacher (Admin Only)
*To use these commands, the teacher must send them directly to the bot via Direct Message.*

* `/ask <question_number> <text>` — Sends an anonymous DM to the student who asked a specific question to request clarification.
* `/delete <question_number>` — Deletes a posted question from the group channel.
* `/lookup <question_number>` — Reveals the identity (Name, Username, ID) of the student who submitted a specific question.
* `/pending` — Lists all questions currently awaiting manual admin review.
* `/review <pending_id>` — Resends the Approve/Reject inline buttons for a pending question.
* `/ban <user_id>` — Permanently blocks a student from submitting new questions.
* `/unban <user_id>` — Removes a student from the ban list.
* `/filter_stats` — Displays statistics on how many questions the AI filter has flagged.
* `/
