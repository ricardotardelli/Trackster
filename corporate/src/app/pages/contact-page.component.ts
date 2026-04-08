import { Component } from '@angular/core';
import { FormsModule } from '@angular/forms';

@Component({
  standalone: true,
  imports: [FormsModule],
  template: `
    <section class="section">
      <div class="container contact-layout">
        <div class="section-header">
          <p class="section-kicker">Contact</p>
          <h1 class="section-title">Get in touch about Trackster</h1>
          <p class="section-text">
            Use the form below to prepare a message. The button opens your email client with the fields already populated.
          </p>

          <div class="surface-card contact-info">
            <h3>Direct contact</h3>
            <a class="inline-link" [href]="'mailto:' + emailAddress">{{ emailAddress }}</a>
            <p>Production workspace: <a class="inline-link" href="https://studio.trackster.pt">studio.trackster.pt</a></p>
          </div>
        </div>

        <form class="surface-card contact-form" (ngSubmit)="openEmail()" #contactForm="ngForm">
          <div class="field">
            <label for="name">Name</label>
            <input
              id="name"
              name="name"
              [(ngModel)]="name"
              type="text"
              required
              placeholder="Your name"
            >
          </div>

          <div class="field">
            <label for="company">Company</label>
            <input
              id="company"
              name="company"
              [(ngModel)]="company"
              type="text"
              placeholder="Company name"
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
              placeholder="you@company.com"
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
              placeholder="Tell us about your use case"
            ></textarea>
          </div>

          <button class="cta-primary submit-button" type="submit" [disabled]="!contactForm.form.valid">
            Send Message
          </button>
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

    @media (max-width: 900px) {
      .contact-layout {
        grid-template-columns: 1fr;
      }
    }
  `]
})
export class ContactPageComponent {
  public readonly emailAddress = 'contact@trackster.pt';

  public name = '';
  public company = '';
  public email = '';
  public message = '';

  public openEmail(): void {
    const subject = encodeURIComponent(`Trackster inquiry from ${this.name || 'website visitor'}`);
    const body = encodeURIComponent(
      [
        `Name: ${this.name}`,
        `Company: ${this.company || 'Not provided'}`,
        `Email: ${this.email}`,
        '',
        'Message:',
        this.message
      ].join('\n')
    );

    window.location.href = `mailto:${this.emailAddress}?subject=${subject}&body=${body}`;
  }
}
