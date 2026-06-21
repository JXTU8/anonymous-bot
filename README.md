# Telegram Anonymous Q&A Bot (Classroom Edition)

This is a Telegram Anonymous Q&A Bot designed specifically for classroom use[cite: 1]. It provides a safe and moderated space for students to ask questions, with powerful administration tools for the teacher[cite: 1]. 

*Credit: Core logic and architecture developed with assistance from Claude.*

## ✨ Features

*   **Flexible Submissions:** Students can ask questions anonymously or publicly[cite: 1]. The bot accepts text, photos, videos, and voice messages[cite: 1].
*   **Upvote System:** Students can anonymously upvote posted questions directly in the group to show shared interest[cite: 1].
*   **Smart Moderation:** Utilizes an AI filter powered by Groq (Llama-3.3-70b) and Serper to analyze questions and check URL reputation[cite: 1]. High-risk messages (spam or phishing) are not auto-deleted; instead, they are sent to a pending review queue for the teacher[cite: 1].
*   **Clarification Requests:** The teacher can request more context from an anonymous student[cite: 1]. The bot sends the student an anonymous DM, and their reply is relayed directly back to the teacher[cite: 1].
*   **Persistent Data:** Question counts, upvotes, and pending reviews are saved via Upstash Redis, ensuring no data is lost if the bot restarts[cite: 1].
*   **Hosting Ready:** Includes a Flask health server to keep the application running on Render's free tier[cite: 1].

---

## 🤖 Commands List

### 🎓 For Students
*   `/start` — Initiates the prompt to ask a new question[cite: 1].
*   `/cancel` — Cancels the current question draft[cite: 1].
*   `/help` — Displays a user-facing guide on how to use the bot[cite: 1].

### 🧑‍🏫 For the Teacher (Admin Only)
*To use these commands, the teacher must send them directly to the bot via Direct Message.*

*   `/ask <question_number> <text>` — Sends an anonymous DM to the student who asked a specific question to request clarification[cite: 1].
*   `/delete <question_number>` — Deletes a posted question from the group channel[cite: 1].
*   `/lookup <question_number>` — Reveals the identity (Name, Username, ID) of the student who submitted a specific question[cite: 1].
*   `/pending` — Lists all questions currently awaiting manual admin review[cite: 1].
*   `/review <pending_id>` — Resends the Approve/Reject inline buttons for a pending question[cite: 1].
*   `/ban <user_id>` — Permanently blocks a student from submitting new questions[cite: 1].
*   `/unban <user_id>` — Removes a student from the ban list[cite: 1].
*   `/filter_stats` — Displays statistics on how many questions the AI filter has flagged[cite: 1].
*   `/getid` — Returns the Chat ID of the current chat (useful for finding your `ADMIN_CHAT_ID` or the `GROUP_CHAT_ID`)[cite: 1].

*(Note: Teachers can answer questions simply by replying to the bot's post directly in the classroom group chat—no special command is needed for this.)*[cite: 1]
