import { CommonModule } from '@angular/common';
import { Component, EventEmitter, Input, Output } from '@angular/core';
import { MatButtonModule } from '@angular/material/button';
import { MatDialogModule } from '@angular/material/dialog';
import { MatIconModule } from '@angular/material/icon';

@Component({
  selector: 'app-dialogshell',
  standalone: true,
  imports: [CommonModule, MatDialogModule, MatButtonModule, MatIconModule],
  templateUrl: './dialogshell.component.html',
  styleUrls: ['./dialogshell.component.css']
})
export class DialogshellComponent {
  @Input() title = '';
  @Input() subtitle = '';
  @Input() showFooter = false;
  @Input() contentClass = '';

  @Output() closeClicked = new EventEmitter<void>();
}