import { Component } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { FormsModule } from '@angular/forms';
@Component({
  selector: 'app-chat',
  imports: [FormsModule],
  templateUrl: './chat.component.html',
  styleUrls: ['./chat.component.css']
})
export class ChatComponent {

  input: string = '';
  loading: boolean = false;

  messages: { role: 'user' | 'assistant', text: string }[] = [];

  constructor(private http: HttpClient) {}

  send() {
    if (!this.input.trim() || this.loading) return;

    const userMessage = this.input;

    
    this.messages.push({ role: 'user', text: userMessage });
    this.input = '';
    this.loading = true;

    this.http.post<any>('/api/chat', { message: userMessage })
      .subscribe({
        next: (res) => {
          this.messages.push({
            role: 'assistant',
            text: res.answer ?? 'Aucune réponse'
          });
          this.loading = false;
        },
        error: () => {
          this.messages.push({
            role: 'assistant',
            text: 'Erreur API'
          });
          this.loading = false;
        }
      });
  }
}
