import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

@Injectable({ providedIn: 'root' })
export class ChatService {
  private apiUrl = 'http://127.0.0.1:8001/chat';

  constructor(private http: HttpClient) {}

  ask(message: string): Observable<any> {
    return this.http.post(this.apiUrl, { message });
  }
}
