import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ChatService, ChatResponse } from './chat.service';

type Msg = { role: 'user' | 'assistant'; text: string };

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule],
  template: `
    <div style="max-width:900px;margin:24px auto;font-family:system-ui;">
      <h2>Chat Test</h2>

      <div style="border:1px solid #ddd;border-radius:12px;padding:12px;height:60vh;overflow:auto;">
        <div *ngFor="let m of messages" style="margin:10px 0;">
          <b>{{ m.role }}:</b>
          <span>{{ m.text }}</span>
        </div>
      </div>

      <form (ngSubmit)="send()" style="display:flex;gap:8px;margin-top:12px;">
        <input [(ngModel)]="input" name="input" required placeholder="Tape ta question..."
               style="flex:1;padding:10px;border-radius:10px;border:1px solid #ddd;">
        <button type="submit" [disabled]="loading"
                style="padding:10px 14px;border-radius:10px;border:1px solid #ddd;">
          {{ loading ? '...' : 'Envoyer' }}
        </button>
      </form>

      <div *ngIf="lastUsed" style="margin-top:10px;color:#666;font-size:12px;">
        used: {{ lastUsed | json }}
      </div>
    </div>
  `
})
export class AppComponent {
  input = '';
  loading = false;
  messages: Msg[] = [];
  lastUsed: any = null;

  constructor(private chat: ChatService) {}

  send() {
    const text = this.input.trim();
    if (!text) return;

    this.messages.push({ role: 'user', text });
    this.input = '';
    this.loading = true;

    this.chat.send(text, true).subscribe({
      next: (res: ChatResponse) => {
        this.messages.push({ role: 'assistant', text: res.answer ?? '—' });
        this.lastUsed = res.used ?? null;
        this.loading = false;
      },
      error: (err) => {
        this.messages.push({ role: 'assistant', text: 'Erreur API' });
        this.lastUsed = err?.error ?? null;
        this.loading = false;
      }
    });
  }
}
