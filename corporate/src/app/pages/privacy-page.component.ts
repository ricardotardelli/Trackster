import { Component } from '@angular/core';

@Component({
  standalone: true,
  template: `
    <section class="section">
      <div class="container">
        <div class="surface-card privacy-card">
          <p class="section-kicker">Privacy</p>
          <h1 class="section-title">Privacy notice</h1>
          <p>
            This website is intended to present Trackster and provide a contact path for commercial or product-related inquiries.
          </p>
          <p>
            If you contact Trackster by email or through the contact form flow, the information you provide may be used to respond to your inquiry and continue relevant business communication.
          </p>
          <p>
            If you need a formal privacy policy for production use, replace this placeholder page with your approved legal text before public launch.
          </p>
        </div>
      </div>
    </section>
  `,
  styles: [`
    .privacy-card {
      padding: 28px;
    }

    .privacy-card p {
      margin: 0 0 16px;
      color: #475569;
      line-height: 1.8;
    }

    .privacy-card p:last-child {
      margin-bottom: 0;
    }
  `]
})
export class PrivacyPageComponent {}
