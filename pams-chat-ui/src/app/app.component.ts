import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { ChatService } from './chat.service';

@Component({
  selector: 'app-root',
  standalone: true,
  imports: [CommonModule, FormsModule],
  template: `
    <div class="chat">
      <h2>Test Chatbot</h2>

      <textarea [(ngModel)]="question" rows="3"></textarea>

      <button (click)="send()">Envoyer</button>

      <div class="answer" *ngIf="answer">
        <strong>Réponse :</strong>
        <pre>{{ answer }}</pre>
      </div>
    </div>
  `,
  styles: [`
    .chat { width: 500px; margin: 40px auto; font-family: Arial; }
    textarea { width: 100%; }
    button { margin-top: 10px; }
    .answer { margin-top: 20px; background: #f5f5f5; padding: 10px; }
  `]
})
export class AppComponent {
  question = '';
  answer = '';

  constructor(private chat: ChatService) {}

  send() {
    this.answer = '⏳ En cours...';
    this.chat.ask(this.question).subscribe({
      next: res => this.answer = res.answer ?? JSON.stringify(res, null, 2),
      error: () => this.answer = '❌ Erreur API'
    });
  }
}
