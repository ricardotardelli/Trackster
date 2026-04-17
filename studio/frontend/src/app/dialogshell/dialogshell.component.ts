import { CommonModule } from '@angular/common';
import { Component, EventEmitter, Input, Output, Optional } from '@angular/core';
import { MatDialogRef } from '@angular/material/dialog';
import { MatIconModule } from '@angular/material/icon';

@Component({
  selector: 'app-dialogshell',
  standalone: true,
  imports: [CommonModule, MatIconModule],
  templateUrl: './dialogshell.component.html',
  styleUrls: ['./dialogshell.component.css']
})
export class DialogShellComponent {
  @Input() title = '';
  @Input() subtitle = '';
  @Input() contentClass = '';
  @Input() showFooter = false;

  @Output() closeClicked = new EventEmitter<void>();

  constructor(
    @Optional() private readonly dialogRef: MatDialogRef<unknown>
  ) {}

  onClose(): void {
    if (this.dialogRef) {
      this.dialogRef.close();
      return;
    }

    this.closeClicked.emit();
  }
}