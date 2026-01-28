import { Component } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { HttpClient } from '@angular/common/http';

@Component({
  selector: 'app-chat',
  standalone: true,
  imports: [CommonModule, FormsModule],
  templateUrl: './chat.component.html',
})
export class ChatComponent {
  message = '';
  messages: { role: 'user' | 'bot'; text: string }[] = [];

  constructor(private http: HttpClient) {}

  send() {
    if (!this.message.trim()) return;

    const userMsg = this.message;
    this.messages.push({ role: 'user', text: userMsg });
    this.message = '';

    this.http.post<any>('http://127.0.0.1:8001/chat', {
      message: userMsg
    }).subscribe(res => {
      this.messages.push({ role: 'bot', text: res.answer });
    });
  }
}
