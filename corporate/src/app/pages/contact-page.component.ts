import { Component, inject } from '@angular/core';
import { FormsModule, NgForm } from '@angular/forms';
import { CommonModule } from '@angular/common';
import { ContactService } from '../services/contact.service';

@Component({
  standalone: true,
  imports: [CommonModule, FormsModule],
  template: `
    <section class="section">
      <div class="container contact-layout">
        <div class="section-header">
          <p class="section-kicker">Contact</p>
          <h1 class="section-title">Get in touch about Trackster</h1>
          <p class="section-text">
            Use the form below to contact the Trackster team directly from this page.
          </p>

          <div class="surface-card contact-info">
            <h3>Direct contact</h3>
            <p>All inquiries are handled directly through this form.</p>
            <p>
              Production workspace:
              <a class="inline-link" href="https://studio.trackster.pt">
                studio.trackster.pt
              </a>
            </p>
          </div>
        </div>

        <form class="surface-card contact-form" (ngSubmit)="submitContact(contactForm)" #contactForm="ngForm">
          <div class="field">
            <label for="name">Name</label>
            <input
              id="name"
              name="name"
              [(ngModel)]="name"
              type="text"
              required
              maxlength="120"
              placeholder="Your name"
              [disabled]="sending"
            >
          </div>

          <div class="field">
            <label for="company">Company</label>
            <input
              id="company"
              name="company"
              [(ngModel)]="company"
              type="text"
              maxlength="200"
              placeholder="Company name"
              [disabled]="sending"
            >
          </div>

          <div class="field">
            <label for="email">Email</label>
            <input
              id="email"
              name="email"
              [(ngModel)]="email"
              type="email"
              required
              email
              maxlength="200"
              placeholder="you@company.com"
              [disabled]="sending"
            >
          </div>

          <div class="field">
            <label for="message">Message</label>
            <textarea
              id="message"
              name="message"
              [(ngModel)]="message"
              rows="7"
              required
              maxlength="3000"
              placeholder="Tell us about your use case"
              [disabled]="sending"
            ></textarea>
          </div>

          <input
            class="honeypot"
            type="text"
            name="website"
            [(ngModel)]="website"
            tabindex="-1"
            autocomplete="off"
          >

          <button class="cta-primary submit-button" type="submit" [disabled]="!contactForm.form.valid || sending">
            {{ sending ? 'Sending...' : 'Send Message' }}
          </button>

          <div class="form-status" *ngIf="successMessage || errorMessage">
            <p class="form-success" *ngIf="successMessage">{{ successMessage }}</p>
            <p class="form-error" *ngIf="errorMessage">{{ errorMessage }}</p>
          </div>
        </form>
      </div>
    </section>
  `,
  styles: [`
    .contact-layout {
      display: grid;
      grid-template-columns: 0.95fr 1.05fr;
      gap: 24px;
      align-items: start;
    }

    .contact-info,
    .contact-form {
      padding: 24px;
    }

    .contact-info {
      margin-top: 26px;
    }

    .contact-info h3 {
      margin: 0 0 12px;
      color: #0b1f44;
    }

    .contact-info p {
      margin: 12px 0 0;
      color: #475569;
      line-height: 1.7;
    }

    .contact-form {
      display: grid;
      gap: 18px;
    }

    .submit-button[disabled] {
      cursor: not-allowed;
      opacity: 0.65;
      transform: none;
    }

    .form-status {
      min-height: 24px;
    }

    .form-success {
      margin: 4px 0 0;
      color: #166534;
      font-weight: 600;
    }

    .form-error {
      margin: 4px 0 0;
      color: #b91c1c;
      font-weight: 600;
    }

    .honeypot {
      position: absolute;
      left: -9999px;
      width: 1px;
      height: 1px;
      opacity: 0;
      pointer-events: none;
    }

    @media (max-width: 900px) {
      .contact-layout {
        grid-template-columns: 1fr;
      }
    }
  `]
})
export class ContactPageComponent {
  private readonly contactService = inject(ContactService);

  public name = '';
  public company = '';
  public email = '';
  public message = '';
  public website = '';

  public sending = false;
  public successMessage = '';
  public errorMessage = '';

  public submitContact(contactForm: NgForm): void {
    if (contactForm.invalid || this.sending) {
      contactForm.form.markAllAsTouched();
      return;
    }

    this.sending = true;
    this.successMessage = '';
    this.errorMessage = '';

    this.contactService.sendContact({
      name: this.name.trim(),
      company: this.company.trim(),
      email: this.email.trim(),
      message: this.message.trim(),
      website: this.website.trim()
    }).subscribe({
      next: (response: any) => {
        this.sending = false;
        this.successMessage =
          response?.message || 'Your message has been sent successfully.';
        this.errorMessage = '';
        this.name = '';
        this.company = '';
        this.email = '';
        this.message = '';
        this.website = '';
        contactForm.resetForm();
      },
      error: (error) => {
        this.sending = false;
        this.successMessage = '';
        this.errorMessage =
          error?.error?.error ||
          error?.error?.message ||
          'Failed to send message.';
      }
    });
  }
}