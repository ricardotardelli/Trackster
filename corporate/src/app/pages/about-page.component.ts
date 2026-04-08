import { Component } from '@angular/core';

@Component({
  standalone: true,
  template: `
    <section class="section">
      <div class="container about-layout">
        <div class="section-header about-copy">
          <p class="section-kicker">About</p>
          <h1 class="section-title">Trackster is built around practical simulation needs</h1>
          <p class="section-text">
            Trackster exists to make CAN simulation workflows more structured, repeatable, and operationally useful.
            Instead of treating simulation as a collection of disconnected engineering tasks, the platform turns it into a productized workflow with clearer control and secure access.
          </p>
          <p class="section-text">
            The focus is simple: help teams create useful synthetic data with realistic control over configuration, timing, routing, and output behavior.
          </p>
        </div>

        <div class="surface-card about-card">
          <h2>What Trackster stands for</h2>
          <ul>
            <li>Clear configuration over scattered setup</li>
            <li>Repeatable workflows over ad hoc execution</li>
            <li>Product-grade access over fragile tool chains</li>
            <li>Practical utility over abstract platform language</li>
          </ul>
        </div>
      </div>
    </section>

    <section class="section">
      <div class="container">
        <div class="card-grid three">
          <article class="surface-card value-card">
            <h3>Precision</h3>
            <p>Simulation settings should be controlled and explicit, not improvised at runtime.</p>
          </article>
          <article class="surface-card value-card">
            <h3>Clarity</h3>
            <p>Users should work from a clean operational interface rather than assembling flows from raw components.</p>
          </article>
          <article class="surface-card value-card">
            <h3>Reliability</h3>
            <p>Repeatability matters when synthetic data is feeding testing, ingestion, or validation workflows.</p>
          </article>
        </div>
      </div>
    </section>
  `,
  styles: [`
    .about-layout {
      display: grid;
      grid-template-columns: 1.2fr 0.8fr;
      gap: 24px;
      align-items: start;
    }

    .about-copy .section-text + .section-text {
      margin-top: 16px;
    }

    .about-card,
    .value-card {
      padding: 24px;
    }

    .about-card h2,
    .value-card h3 {
      margin: 0 0 12px;
      color: #0b1f44;
    }

    .about-card ul {
      margin: 0;
      padding-left: 18px;
      line-height: 1.9;
      color: #475569;
    }

    .value-card p {
      margin: 0;
      line-height: 1.75;
      color: #475569;
    }

    @media (max-width: 900px) {
      .about-layout {
        grid-template-columns: 1fr;
      }
    }
  `]
})
export class AboutPageComponent {}
