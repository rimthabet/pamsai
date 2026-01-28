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
export class ChatApiService {
  private baseUrl = 'http://127.0.0.1:8001';

  constructor(private http: HttpClient) {}

  chat(message: string, debug = false): Observable<ChatResponse> {
    return this.http.post<ChatResponse>(
      `${this.baseUrl}/chat`,
      { message, debug },
      { headers: { 'Content-Type': 'application/json; charset=utf-8' } }
    );
  }
}
