import { CommonModule } from '@angular/common';
import { Component, Inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';

@Component({
  selector: 'app-payload-preview',
  standalone: true,
  imports: [CommonModule],
  templateUrl: './payload.component.html',
  styleUrls: ['./payload.component.css']
})
export class PayloadComponent {
  payload: string = '';

  constructor(
    private dialogRef: MatDialogRef<PayloadComponent>,
    @Inject(MAT_DIALOG_DATA) public data: any
  ) {
    this.payload = data.payload || '';
  }

  public close(): void {
    this.dialogRef.close();
  }

  public copyPayload(): void {
    if (!this.payload) return;

    navigator.clipboard.writeText(this.payload).catch(() => {
      // silent
    });
  }
}