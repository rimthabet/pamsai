import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

export type ChatResponse = {
  answer: string;
  sources?: any[];
  suggested_actions?: any[];
  navigation?: any[];
  used?: any;
};

@Injectable({ providedIn: 'root' })
export class ChatService {
  private apiUrl = 'http://127.0.0.1:8001/chat';

  constructor(private http: HttpClient) {}

  send(message: string, debug = false): Observable<ChatResponse> {
    return this.http.post<ChatResponse>(this.apiUrl, { message, debug });
  }
}
