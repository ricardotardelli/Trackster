import { CommonModule } from '@angular/common';
import { Component, Inject } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatButtonModule } from '@angular/material/button';
import { MatIconModule } from '@angular/material/icon';
import { DialogshellComponent } from '../dialogshell/dialogshell.component';

interface PayloadDialogData {
  payloadText: string;
}

@Component({
  selector: 'app-payloadmodule',
  standalone: true,
  imports: [
    CommonModule,
    MatDialogModule,
    MatButtonModule,
    MatIconModule,
    DialogshellComponent
  ],
  templateUrl: './payload.component.html',
  styleUrls: ['./payload.component.css']
})
export class PayloadComponent {
  payloadText: string = '';

  constructor(
    private dialogRef: MatDialogRef<PayloadComponent>,
    @Inject(MAT_DIALOG_DATA) private data: PayloadDialogData
  ) {
    this.payloadText = data?.payloadText ?? '';
  }

  closeDialog(): void {
    this.dialogRef.close();
  }

  copyPayload(): void {
    navigator.clipboard.writeText(this.payloadText || '');
  }
}