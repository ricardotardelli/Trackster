import { Injectable, inject } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

export interface ContactPayload {
  name: string;
  company?: string;
  email: string;
  message: string;
}

@Injectable({
  providedIn: 'root'
})
export class ContactService {
  private readonly http = inject(HttpClient);
  private readonly contactApiUrl = 'https://fyg20pi4vk.execute-api.us-east-1.amazonaws.com/contact';

  public sendContact(payload: ContactPayload): Observable<unknown> {
    return this.http.post(this.contactApiUrl, payload);
  }
}